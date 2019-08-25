import random
import math

import Const
import ClusterCreator
import Leader
import Cluster
import InsertionBuffer
import OSBuffer

class ExtCP:
    def __init__(self, io, setup):
        # Read setup params for simulation
        self.io = io
        self.size = setup['Initial_Size']
        self.size_target = setup['IO_Size']
        self.descriptor_size = setup['Descriptor_Size']
        self.extra_leaders = setup['Recluster_Pct']
        self.flush_method = setup['Flush_Method']
        self.buffer_size = setup['Buffer_Size']
        self.ins_buffer_size = setup['Ins_Buffer_Size']
        self.os_buffer_size = setup['OS_Buffer_Size']
        self.os_frame_size = setup['OS_Frame_Size']
        
        self.leader_size_strategy = setup['Leader_Size_Strategy']
        self.cluster_size_strategy = setup['Cluster_Size_Strategy']
        self.target_leader_size = setup['Leader_Target_Size']
        self.lo_pct = setup['LO_Size_Pct']
        self.hi_pct = setup['HI_Size_Pct']

        # floor(cluster size / descriptor size)
        ## denotes the amount of descriptors per cluster in our optimal scenario
        self.target_cluster_size = math.floor(self.size_target / self.descriptor_size)

        # Create the buffer manager
        if self.buffer_size != 0:
            self.ins_buffer = InsertionBuffer.InsertionBuffer(self.ins_buffer_size)
            self.os_buffer = OSBuffer.OSBuffer(self.os_buffer_size, self.os_frame_size)

        # Init creator
        self.cc = ClusterCreator.ClusterCreator(self.size, 
                                                self.target_leader_size,
                                                self.lo_pct,
                                                self.hi_pct,
                                                self.extra_leaders, 
                                                self.target_cluster_size,
                                                )

        # create init clustering
        self.leaders, self.clusters, self.l, self.count_id = self.cc.create_clusters()

    # ----- GENERIC INTERFACE METHODS -----

    def insert(self):
        # choose random cluster
        '''
        Uses the random.random() generator as it is much faster than random.randrange(a,b) or random.randint(a,b).
        Thanks to Eli Bendersky's investigation on the matter:
        https://eli.thegreenplace.net/2018/slow-and-fast-methods-for-generating-random-integers-in-python/#id4
        '''
        rn = int(len(self.clusters) * random.random())

        chosen_cluster = self.clusters[rn]
        cluster_leader = chosen_cluster.leader
        org_size = math.ceil(chosen_cluster.count/self.target_cluster_size)

        # If our cluster policy is absolute size
        if self.cluster_size_strategy == Const.SIZE_ABSOLUTE:
            # If cluster is at or above target size:
            if chosen_cluster.count >= self.target_cluster_size:
                # If we have multiple levels of height in leader tree
                if self.l > 1:
                    # RECLUSTER WITH CLUSTER REP.'s LEAD,
                    self.recluster_clusters(cluster_leader.leader)

                    if self.buffer_size == 0:
                        # No buffering, do direct insertion
                        # Account for original read cost (Leaf RR * potential oversize)
                        self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                        # Then account for IO after recluster (Leaf RW)
                        self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(chosen_cluster.count+1/self.target_cluster_size))
                    else:
                        # Here we take care not to overwrite a previous flush
                        triple = self.ins_buffer.insert_feature_leader(cluster_leader)

                # We have one level of height in tree, but cluster is too big
                # We need to recluster, but have no leader -> we must create a new leader layer
                else:
                    # Create a new leader level
                    self.create_new_leader_level(cluster_leader)

                    # With our new leader level, we can now ask to recluster
                    self.recluster_clusters(cluster_leader.leader)
                    
                    if self.buffer_size == 0:
                        # No buffering, do direct insertion
                        # Account for original read cost (Leaf RR * potential oversize)
                        self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                        # Then account for IO after recluster (Leaf RW)
                        self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(chosen_cluster.count+1/self.target_cluster_size))
                    else:
                        # Here we take care not to overwrite a previous flush
                        triple = self.ins_buffer.insert_feature_leader(cluster_leader)

            # If not above:
            else:
                if self.buffer_size == 0:
                    # No buffering, do direct insertion
                    # Account for read cost (Leaf RR)
                    self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, math.ceil(chosen_cluster.count+1/self.target_cluster_size))
                    # Then account for IO (Leaf RW)
                    self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(chosen_cluster.count+1/self.target_cluster_size))
                else:
                    # Here we take care not to overwrite a previous flush
                    triple = self.ins_buffer.insert_feature_leader(cluster_leader)

        # If our cluster policy is average size
        elif self.cluster_size_strategy == Const.SIZE_AVERAGE:
            # If cluster is above target size
            if chosen_cluster.count >= self.target_cluster_size:
                # If we have multiple levels of height in leader tree
                if self.l > 1:
                    # If the cluster leader's leader confirms that the average is above target cluster size
                    # Or if our count is greater than 2* the target size
                    if self.check_avg_cluster_size(cluster_leader.leader) or chosen_cluster.count > (self.target_cluster_size * 2):
                        # Ask our cluster representative's leader to recluster us
                        self.recluster_clusters(cluster_leader.leader)

                        if self.buffer_size == 0:
                            # No buffering, do direct insertion
                            # Account for original read cost (Leaf RR * potential oversize)
                            self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                            # Then account for IO after recluster (Leaf RW)
                            self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil((chosen_cluster.count+1)/self.target_cluster_size))
                        else:
                            # Here we take care not to overwrite a previous flush
                            triple = self.ins_buffer.insert_feature_leader(cluster_leader)

                    # if we do not need to recluster yet
                    else:
                        if self.buffer_size == 0:
                            # No buffering, do direct insertion
                            # Account for original read cost (Leaf RR * potential oversize)
                            self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                            # Then account for IO (Leaf RW, potential oversize)
                            self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil((chosen_cluster.count +1)/self.target_cluster_size))
                        else:
                            # Here we take care not to overwrite a previous flush
                            triple = self.ins_buffer.insert_feature_leader(cluster_leader)
                # We have one level of height in tree, but cluster is too big
                # We need to recluster, but have no leader -> we must create a new leader layer
                else:
                    # Create a new leader level
                    self.create_new_leader_level(cluster_leader)

                    # If the new cluster leader's leader confirms that the average is above target cluster size
                    # Or if our count is greater than 2* the target size
                    if self.check_avg_cluster_size(cluster_leader.leader) or chosen_cluster.count > (self.target_cluster_size * 2):
                        # With our new leader level, we can now ask to recluster
                        self.recluster_clusters(cluster_leader.leader)

                        if self.buffer_size == 0:
                            # No buffering, do direct insertion
                            # Account for original read cost (Leaf RR * potential oversize)
                            self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                            # Then account for IO after recluster (Leaf RW)
                            self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil((chosen_cluster.count+1)/self.target_cluster_size))
                        else:
                            # Here we take care not to overwrite a previous flush
                            triple = self.ins_buffer.insert_feature_leader(cluster_leader)

                    # if we do not need to recluster yet
                    else:
                        if self.buffer_size == 0:
                            # No buffering, do direct insertion
                            # Account for original read cost (Leaf RR * potential oversize)
                            self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, org_size)
                            # Then account for IO (Leaf RW, potential oversize)
                            self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil((chosen_cluster.count +1)/self.target_cluster_size))
                        else:
                            # Here we take care not to overwrite a previous flush
                            triple = self.ins_buffer.insert_feature_leader(cluster_leader)
            # If not above target size:
            else:
                if self.buffer_size == 0:
                    # No buffering, do direct insertion
                    # Account for read cost (Leaf RR)
                    self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, 1)
                    # Then account for IO (Leaf RW)
                    self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, 1)
                else:
                    # Here we take care not to overwrite a previous flush
                    triple = self.ins_buffer.insert_feature_leader(cluster_leader)
        # Else: Not defined, raise an error
        else:
            raise NotImplementedError("Add new cluster size strategy")

        # If we have a buffer and we flushed from an insertion (no matter what path we chose above)
        if self.buffer_size > 0 and triple[0]:
            if self.flush_method == Const.FLUSH_SCAN:
                # io cost for scanning all clusters
                for c in self.clusters:
                    leader_id = c.leader.identity
                    #check if in OS buffer
                    if self.os_buffer.is_in_set(leader_id):
                        pass
                    else:
                        self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, math.ceil(c.count / self.target_cluster_size))
                    
                    # Add or update cluster to buffer
                    os_flushed = self.os_buffer.set(leader_id, (c.count+1) * self.descriptor_size)

                    # Add write cost for any potential flushed clusters or leaders
                    for j in os_flushed:
                        self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(j / self.size_target))

                # then clear leaves bookkeeping
                self.ins_buffer.clear_leaves()
            else:
                # io cost for random read/write of clusters that were modified (uses stored leaders)
                leads = self.ins_buffer.return_leaves()
                for i in leads:
                    c = i.cluster[0]
                    #check if in OS buffer
                    if self.os_buffer.is_in_set(i.identity):
                        pass
                    else:
                        self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, math.ceil(c.count / self.target_cluster_size))

                    # Add or update cluster to buffer
                    os_flushed = self.os_buffer.set(i.identity, (c.count+1) * self.descriptor_size)

                    # Add write cost for any potential flushed clusters or leaders
                    for j in os_flushed:
                        self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(j / self.size_target))

                # then clear leaves bookkeeping
                self.ins_buffer.clear_leaves()
        # Finally, insert
        chosen_cluster.insert()
        self.size +=1

    def get_index_stats(self):
        t_d = sorted(self.leaders, key=lambda x: x.height, reverse=True)
        util = self.get_util()
        min_l, max_l, avg_l = self.get_leader_stats()
        min_c, max_c, avg_c = self.get_cluster_stats()

        return [
            '{0:s}\t{1:d}'.format("ECP_Size", self.size),
            '{0:s}\t{1:d}'.format("ECP_L", self.l),
            '{0:s}\t{1:d}'.format("ECP_Max_Height", t_d[0].height),
            '{0:s}\t{1:f}'.format("ECP_Utilization", util),
            '{0:s}\t{1:d}'.format("Leader_Count", len(self.leaders)),
            '{0:s}\t{1:d}'.format("Min_Leader_Size", min_l),
            '{0:s}\t{1:d}'.format("Max_Leader_Size", max_l),
            '{0:s}\t{1:f}'.format("Avg_Leader_Size", avg_l),
            '{0:s}\t{1:d}'.format("Cluster_Count", len(self.clusters)),
            '{0:s}\t{1:d}'.format("Min_Cluster_Size", min_c),
            '{0:s}\t{1:d}'.format("Max_Cluster_Size", max_c),
            '{0:s}\t{1:f}'.format("Avg_Cluster_Size", avg_c)
        ]

    # Used to clear buffer with when simulation ends.
    def clear_osbuf(self):
        if self.buffer_size > 0:
            os_flushed = self.os_buffer.flush_buffer()
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_FLUSH, math.ceil(j / self.size_target))
        else:
            pass

    # ----- STATS -----
    def get_leader_stats(self):
        ls = sorted(self.leaders, key=lambda x: x.size, reverse=True)
        max_l = ls[0].size
        min_l = ls[len(ls)-1].size
        sum_size = 0
        for i in ls:
            sum_size += i.size
        
        avg_l = 1.0 * sum_size / len(ls)
        return min_l, max_l, avg_l

    def get_cluster_stats(self):
        cs = sorted(self.clusters, key=lambda x: x.count, reverse=True)
        max_c = cs[0].count
        min_c = cs[len(cs)-1].count
        sum_size = 0
        for i in cs:
            sum_size += i.count
        
        avg_c = 1.0 * sum_size / len(cs)
        return min_c, max_c, avg_c

    def get_util(self):
        total = 0
        for i in self.clusters:
            total += i.count
        return 1.0 * total / (len(self.clusters) * self.target_cluster_size)

    # ----- SPECIFIC INDEX METHODS -----

    def check_avg_cluster_size(self, leader):
        # Count how many subleaders we have
        num_subleads = len(leader.cluster)

        # Count the subleaders' cluster sizes
        sum_cluster_sizes = sum(x.cluster[0].count for x in leader.cluster)

        #calc average
        avg_size = sum_cluster_sizes/num_subleads

        # If average cluster size is above target cluster size (1 IO), we return true
        if avg_size > self.target_cluster_size:
            return True
        # Else we are fine (we defer reclustering) and return false
        else:
            return False

    def check_avg_leader_size(self, leader):
        # Count how many subleaders we have
        num_subleads = len(leader.cluster)

        # Count the subleaders' cluster sizes
        sum_cluster_sizes = sum(x.size for x in leader.cluster)

        # calculate the average subleader cluster size
        avg_size = sum_cluster_sizes/num_subleads

        # If average cluster size is above target leader size, we return true
        if avg_size > self.target_leader_size:
            return True
        # Else we are fine (we defer reclustering) and return false
        else:
            return False

    def decide_if_leader_recluster(self, leader):
        # We cannot be above our max capacity, need to recluster
        if self.leader_size_strategy == Const.LEAD_SIZE_ABSOLUTE:
            # After reclustering, check if we are above our allowed size (capacity)
            # If so, we also need to be reclustered by our leader
            if len(leader.cluster) > leader.max_capacity:
                self.recluster_leaders(leader)

        # the average size for all our leader's clusters cannot be above the max capacity
        elif self.leader_size_strategy == Const.LEAD_SIZE_AVERAGE:
            # After reclustering, check if we are above our allowed size (capacity)
            if len(leader.cluster) > leader.max_capacity:
                # Check if we are top leader (L is equal to our height)
                if self.l == leader.height:
                    # Create new leader
                    self.create_new_leader_level(leader)
                    # then ask if we are to be resized by our new leader
                    if self.check_avg_leader_size(leader.leader):
                        self.recluster_leaders(leader)
                    # Failover: we must not be greater than 2 times our max capacity
                    if len(leader.cluster) > (leader.max_capacity *2):
                        self.recluster_leaders(leader)
                else:
                    # If so, check if we are to be resized by our leader
                    if self.check_avg_leader_size(leader.leader):
                        self.recluster_leaders(leader)
                    # Failover: we must not be greater than 2 times our max capacity
                    if len(leader.cluster) > (leader.max_capacity *2):
                        self.recluster_leaders(leader)

        # Else: Not defined, raise an error
        else:
            raise NotImplementedError("Add new leader size strategy")

    def recluster_clusters(self, leader):
        # Calc IO read cost for subleaders and their clusters
        for x in leader.cluster:
            c = x.cluster[0]
            #check if in OS buffer
            if self.os_buffer.is_in_set(x.identity):
                pass
            else:
                self.io.add_io_cost(Const.L_CLRR, Const.C_RECLUSTER, math.ceil(c.count / self.target_cluster_size))


            # Add or update cluster to buffer
            os_flushed = self.os_buffer.set(x.identity, c.count * self.descriptor_size)

            # Add write cost for any potential flushed clusters or leaders
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

        # Count the subleader's cluster sizes
        ## NOTE: x <- cluster leader representative, y <- the Cluster
        sum_cluster_sizes = sum(y.count for x in leader.cluster for y in x.cluster)

        # original leader amount
        org_lead_amount = len(leader.cluster)

        # How many leaders we will require
        target_leaders = math.ceil(sum_cluster_sizes/self.target_cluster_size)

        # resize such that clusters are not near full capacity
        target_leaders = math.ceil((target_leaders/100)*(100 + self.lo_pct))

        # reset cluster sizes for all clusters
        for x in leader.cluster:
            x.cluster[0].count = 1

        # subtract the leader representatives' descriptors (which we will keep and not recluster)
        sum_cluster_sizes = sum_cluster_sizes - org_lead_amount

        #calc new amount of added leaders (and their clusters), minus existing leaders
        to_add = target_leaders - org_lead_amount

        #create new amount of cluster representatives
        for x in range(to_add):
            new_sublead = Leader.Leader(i=self.count_id, height=leader.height-1, leader=leader)
            self.count_id += 1
            new_sublead.represents_cluster = True
            cluster = Cluster.Cluster(leader=new_sublead)
            new_sublead.cluster.append(cluster)

            leader.cluster.append(new_sublead)
            leader.size +=1
            self.leaders.append(new_sublead)
            self.clusters.append(cluster)

        # Minus the new cluster representatives (which we will have taken from sum)
        sum_cluster_sizes = sum_cluster_sizes - to_add

        #recluster remaining sum into the new amount of clusters
        for x in range(sum_cluster_sizes):
            # Choose random cluster representative
            rn = int(len(leader.cluster) * random.random())
            # Access the cluster representative's cluster and call it's insert method
            leader.cluster[rn].insert_into_cluster()

        # update leaders and their clusters in os_buffer with the new sizes
        for x in leader.cluster:
            c = x.cluster[0]
            #check if in OS buffer
            if self.os_buffer.is_in_set(x.identity):
                pass
            else:
                self.io.add_io_cost(Const.L_CLRR, Const.C_RECLUSTER, math.ceil(c.count / self.target_cluster_size))

            # Add or update cluster to buffer
            os_flushed = self.os_buffer.set(x.identity, c.count * self.descriptor_size)

            # Add write cost for any potential flushed clusters or leaders
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

            # Delete all the data for these subleaders (and their clusters) from ins_buffer
            # As we have just done a complete reclustering of them (which means the buffered objects are meaningless now)
            self.ins_buffer.delete(x.identity)

        ## Reclustering of leaders if above max_capacity here
        self.decide_if_leader_recluster(leader)

    def recluster_leaders(self, leader):
        # Check if given leader is a top leader
        if leader.leader == None:
            # create new top leader
            self.create_new_leader_level(leader)
            
            # run this method again with same leader (recurse), as we should now have a leader
            self.recluster_leaders(leader)
        # leader is not top leader, let's have our leader recluster us
        else:
            our_leader = leader.leader

            if self.os_buffer.is_in_set(our_leader.identity):
                pass
            else:
                # add read cost of our_leader and its cluster
                self.io.add_io_cost(Const.L_CLRR, Const.C_RECLUSTER, (our_leader.size * self.descriptor_size / self.size_target))

            # Add or update leader to buffer
            os_flushed = self.os_buffer.set(our_leader.identity, our_leader.size * self.descriptor_size)

            # Add write cost for any potential flushed clusters or leaders
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

            num_subleads = len(our_leader.cluster)
            sum_leader_sizes = sum(x.size for x in our_leader.cluster)

            # add read cost of unbuffered subleaders and potential LRU write cost
            for i in our_leader.cluster:
                #check if in OS buffer
                if self.os_buffer.is_in_set(i.identity):
                    pass
                else:
                    # add read cost of one subleader
                    self.io.add_io_cost(Const.L_CLRR, Const.C_RECLUSTER, (i.size * self.descriptor_size / self.size_target))
                
                # Add or update leader to buffer
                os_flushed = self.os_buffer.set(i.identity, i.size * self.descriptor_size)

                # Add write cost for any potential flushed clusters or leaders
                for j in os_flushed:
                    self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

            #how many leaders our subleaders should have after resize (their 'lo' target)
            sub_leader_target = math.ceil((self.target_leader_size/100)*(100-self.lo_pct))

            # how many leaders we need to have in 'our_leader' in order to facilitate the 'sub_leader_target'
            target_leaders = math.ceil(sum_leader_sizes/sub_leader_target)

            # collect all our subleaders' clusters
            # and reset sizes for all subleaders
            concat_sublead_clusters = []

            # for subleader in our leader's cluster
            for x in our_leader.cluster:
                    # take subleader's cluster and concat them to the list (read cost accounted for above)
                    concat_sublead_clusters.extend(x.cluster)

                    # set sublead's clusters to empty
                    x.cluster = []
                    # set his size to 0
                    x.size = 0

            #calc new amount of subleaders to add (which also will take from 'concat_sublead_clusters')
            to_add = target_leaders - num_subleads

            for x in range(to_add):
                # create new sublead
                new_sublead = Leader.Leader(i=self.count_id, height=our_leader.height-1, leader=our_leader)
                self.count_id += 1
                # set his max capacity to target_size + the added extra 'Hi' percent
                new_sublead.max_capacity = math.ceil((self.target_leader_size/100)*(100+self.hi_pct))
                # set his size to 0 (he will get some soon)
                new_sublead.size = 0
                # Add him to our leader's cluster
                our_leader.cluster.append(new_sublead)
                our_leader.size += 1
                self.leaders.append(new_sublead)

            #recluster subleaders' clusters into the new amount of subleaders
            for x in concat_sublead_clusters:
                #select random subleader from our_leader ( no read cost, already read)
                rn = int(len(our_leader.cluster) * random.random())
                selected_subleader = our_leader.cluster[rn]
                # Append the leader to the subleader
                selected_subleader.cluster.append(x)
                # increment his size
                selected_subleader.size +=1
                #Have the cluster updated his new leader (height does not change)
                x.leader = selected_subleader

            # Add write cost for all subleaders ('n + to_add' descriptors, sequential block)
            write_cost_1 = math.ceil(len(our_leader.cluster) * self.descriptor_size / self.size_target)
            self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, write_cost_1)

            # update subleaders in os_buffer and add new ones
            for x in our_leader.cluster:
                #check if in OS buffer (should be, just checked in this function call)
                if self.os_buffer.is_in_set(x.identity):
                    pass
                else:
                    # add read cost of one subleader
                    self.io.add_io_cost(Const.L_CFRR, Const.C_RECLUSTER, (x.size * self.descriptor_size / self.size_target))
                
                # Add or update leader to buffer
                os_flushed = self.os_buffer.set(x.identity, x.size * self.descriptor_size)

                # Add write cost for any potential flushed clusters or leaders
                for j in os_flushed:
                    self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

                
                # update for each of the leaders in the subleader's cluster using the OS buffer
                for i in x.cluster:
                    #check if in OS buffer (should be, just checked in this function call)
                    if self.os_buffer.is_in_set(i.identity):
                        pass
                    else:
                        # add read cost of one subleader
                        self.io.add_io_cost(Const.L_CFRR, Const.C_RECLUSTER, (i.size * self.descriptor_size / self.size_target))
                    
                    # Add or update leader to buffer
                    os_flushed = self.os_buffer.set(i.identity, i.size * self.descriptor_size)

                    # Add write cost for any potential flushed clusters or leaders
                    for j in os_flushed:
                        self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

            # Check if our leader needs a resize after adding the new subleaders
            # (since this may force him above his max capacity)
            self.decide_if_leader_recluster(our_leader)

    def create_new_leader_level(self, leader):
        # get all leaders with the height of given leader
        ## NOTE: 
        ## in actual implementation each 'height' layer of leaders should be kept 
        ## separate and their subleads should be grouped by their leader.
        ## For purposes of simulating IO's this is not critical 
        ## and therefore skipped.
        top_leaders = [x for x in self.leaders if x.height == leader.height]

        # add read cost of unbuffered leaders and potential LRU write cost
        for i in top_leaders:
            #check if in OS buffer
            if self.os_buffer.is_in_set(i.identity):
                pass
            else:
                # add read cost of one subleader
                self.io.add_io_cost(Const.L_CFRR, Const.C_RECLUSTER, (i.size * self.descriptor_size / self.size_target))
            
            # Add or update leader to buffer
            os_flushed = self.os_buffer.set(i.identity, i.size * self.descriptor_size)

            # Add write cost for any potential flushed clusters or leaders
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, math.ceil(j / self.size_target))

        # create new top leader
        new_leader = Leader.Leader(i=self.count_id, height=leader.height+1)
        self.count_id += 1
        # set his max capacity to target_size + the added extra 'Hi' percent
        new_leader.max_capacity = math.ceil((self.target_leader_size/100)*(100+self.hi_pct))
        # make him take all current top leaders as his cluster, and set his length equal to the amount
        new_leader.cluster = top_leaders
        new_leader.size = len(top_leaders)

        # Have all of the previous top leaders assign him as their leader
        for x in top_leaders:
            x.leader = new_leader

        # append him to the general leaders list
        self.leaders.append(new_leader)

        # increment l by 1 (as tree now has grown in height)
        self.l += 1

        # Add IO write cost for 1 new top leader (which is descriptor size)
        self.io.add_io_cost(Const.L_CFRW, Const.C_RECLUSTER, 1)