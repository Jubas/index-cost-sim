#imports
import Cluster
import math

class Leader():
    def __init__(self, i, height, leader=None):
        # represents local level id
        self.identity = i

        # Leader of this leader (if any)
        self.leader = leader

        # height of this leader in the tiers
        self.height = height

        # contains cluster for leader;
        # either a specific cluster or multiple other subleaders
        self.cluster = []

        # T/F value to signify if this leader has a Cluster or not
        self.represents_cluster = False

        # Max capacity for leader - relevant when creating subleads;
        # '1' for leaders representing a cluster
        self.max_capacity = 1

        #Current size for leader;
        # '1' for leaders representing a cluster
        # > 1 for leaders representing other sub-leaders
        self.size = 1

    def sub_clustering(self, identity, per_lead, target_leader_size, hi_pct, height, l):
        tmp_leads = []
        tmp_clusters = []
        if l > 1:
            # set max capacity to target_size + the added extra 'Hi' percent
            self.max_capacity = math.ceil((target_leader_size/100)*(100+hi_pct))

            # set size to target
            self.size = per_lead

            for i in range(per_lead):
                leader = Leader(identity, height, leader=self)
                identity += 1
                self.cluster.append(leader)
                tmp_leads.append(leader)
                leads, clusters, c_id = leader.sub_clustering(identity, per_lead, target_leader_size, hi_pct, height-1, (l-1))
                identity = c_id

                tmp_leads.extend(leads)
                tmp_clusters.extend(clusters)

            return tmp_leads, tmp_clusters, identity

        else:
            self.represents_cluster = True
            cluster = Cluster.Cluster(leader=self)
            self.cluster.append(cluster)
            return tmp_leads, self.cluster, identity

    # Used on cluster creation time to eliminate extra leaders.
    # Can fail badly if used incorrectly.
    def elim_leader(self, i):
        #find index of said leader
        idx = [ix for ix, x in enumerate(self.cluster) if x.identity == i]

        #remove him
        del self.cluster[idx[0]]

        self.size = self.size -1

    # Used to insert into the Cluster from this leader
    def insert_into_cluster(self):
        if self.represents_cluster == True:
            self.cluster[0].insert()
        else:
            raise RuntimeError("Tried calling 'insert_into_cluster' on a leader which does not represent a Cluster")

    def __str__(self):
        return "Leader: " + \
               "Cluster: " + str(self.cluster)
