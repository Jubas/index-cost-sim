#Imports
import math
import random

import Leader

class ClusterCreator:
    def __init__(self, size, target_leader_size, lo_pct, hi_pct, extra_leaders, target_cluster_size):
        #store params
        self.size = size
        self.target_leader_size = target_leader_size
        self.lo_pct = lo_pct
        self.hi_pct = hi_pct
        self.extra_leaders = extra_leaders
        self.target_cluster_size = target_cluster_size
        # unique id for leaders
        self.count_id = 0

    def find_leaders(self):
        #Adjust target cluster size for 'lo'
        adjusted_cluster_size = math.ceil(self.target_cluster_size/100)*(100-self.lo_pct)

        # leaders = ceil(n/ target_cluster_size )
        ## denotes the amount of descriptors per cluster in our optimal scenario
        leaders = math.ceil(self.size/adjusted_cluster_size)

        ## then add pct extra l's
        if self.extra_leaders > 0:
            extra_l = math.ceil((leaders/100)*(100 + self.extra_leaders))
            return extra_l
        else:
            return leaders

    #create clusters
    def create_clusters(self):
        # find n leaders
        lead_amount = self.find_leaders()

        # Calculate 'l' based off the targer_leader_size (assuming random and uniform distribution)
        self.l = math.ceil(math.log(lead_amount)/math.log(self.target_leader_size))

        # recalc the target_leader_size to be more like what 'l' targets for the 'leader' amount
        per_lead = math.ceil(lead_amount**(1/float(self.l)))

        leaders = []
        clusters = []

        #Create structure of leaders (and sub-leaders) and clusters
        height = self.l
        for i in range(per_lead):
            leader = Leader.Leader(i=self.count_id, height=height)
            self.count_id += 1
            leaders.append(leader)
            subleads, subclusters, c_id = leader.sub_clustering(self.count_id, per_lead, self.target_leader_size, self.hi_pct, height-1, self.l)
            self.count_id = c_id

            leaders.extend(subleads)
            clusters.extend(subclusters)

        # calculate existing nodes in index (clusters with leader points)
        l_counts = len(clusters)

        # Insert into clusters, minus the existing leader points 
        # (clusters have default size of 1)
        for i in range((self.size - l_counts)):
            #Select a cluster
            rn = int(len(clusters) * random.random())
            clusters[rn].insert()

        #recluster leaders if extra_leaders > 0
        if self.extra_leaders > 0:
            #calculate clusters to redistribute
            to_elim = math.floor((lead_amount/(100 + self.extra_leaders))*self.extra_leaders)

            # sort clusters by count (ASC order)
            sorted_clusters_asc = sorted(clusters, key=lambda x: x.count)

            #find clusters to eliminate and keep
            elim_clusters = sorted_clusters_asc[:to_elim]
            to_keep = sorted_clusters_asc[to_elim:]

            # count descriptors to redistribute and delete the leader
            redis_points = 0
            for i in elim_clusters:
                redis_points += i.count

                lead_id = i.leader.identity

                # if l > 1 (leaders are in tree-like)
                if i.leader.leader != None:
                    # delete him from his leader
                    i.leader.leader.elim_leader(lead_id)
                    # delete him from full leader list
                    idx = [ix for ix, x in enumerate(leaders) if x == i.leader]
                    del leaders[idx[0]]

                # if l = 1 (leaders are in flat array, height is 1)
                else:
                    #find index of said leader
                    idx = [ix for ix, x in enumerate(leaders) if x.identity == lead_id]
                    #remove him
                    del leaders[idx[0]]

            #redistribute (insert) into clusters
            for i in range(redis_points):
                #Select a cluster
                rn = int(len(to_keep) * random.random())
                to_keep[rn].insert()

            clusters = to_keep
        return leaders, clusters, self.l, self.count_id