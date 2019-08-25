# System imports
import random
import math
import statistics

# User imports
import Partition
import PartitionNode
import InsertionBuffer
import OSBuffer
import LeafLevelCreator
import Const


class NVTree:
    def __init__(self, io, setup):
        # Store parameter values
        self.io = io
        self.size = setup['Initial_Size']
        self.size_target = setup['IO_Size']
        self.descriptor_size = setup['Descriptor_Size']
        self.max_leaves = setup['Max_Leaves']

        self.repro_method = setup['Repro_Method']
        self.split_method =  setup['Split_Method']
        self.flush_method = setup['Flush_Method']
        self.buffer_size = setup['Buffer_Size']
        self.ins_buffer_size = setup['Ins_Buffer_Size']
        self.os_buffer_size = setup['OS_Buffer_Size']
        self.os_frame_size = setup['OS_Frame_Size']

        self.feature_size = self.descriptor_size + 4

        # Create the buffer manager
        if self.buffer_size != 0:
            self.ins_buffer = InsertionBuffer.InsertionBuffer(self.ins_buffer_size)
            self.os_buffer = OSBuffer.OSBuffer(self.os_buffer_size, self.os_frame_size)

        self.use_pfiles = self.repro_method == Const.REPRO_PFILES

        # Create the initial tree as leaves (partitions) and
        # nodes (partition nodes), and get the initial V from example 5
        self.lc = LeafLevelCreator.LeafLevelCreator(self.size,
                                                    self.max_leaves)
        self.leaves, self.nodes = self.lc.create_leaves_and_nodes()
        self.next_id = len(self.leaves)
        self.split_count = 0

    # ----- GENERIC INTERFACE METHODS -----

    # Do the actual insertion
    def insert(self):
        # Get the partition
        identifier = self.partition_to_insert()

        # Insert the value pair to buffers
        (flushed, num_part, num_pfiles) = \
            self.ins_buffer.insert_value_pair(identifier, self.use_pfiles)

        # If ins_buffer were flushed, we now need to account for the costs
        if flushed:
            # First, we account for cost of partitions
            if self.flush_method == Const.FLUSH_SCAN:
                # Either a full scan of the partitions
                self.io.add_io_cost(Const.L_CLSR, Const.L_INSERT,
                                    len(self.leaves))
                self.io.add_io_cost(Const.L_CLSW, Const.L_INSERT,
                                    len(self.leaves))
                self.ins_buffer.clear_values()
            else:
                # Or a read and write (randomly) of partitions in ins_buffer

                #get affected partitions
                par_list = self.ins_buffer.return_values()

                for par in par_list:
                    par_obj = self.leaves[par]
                    # check if in OS buffer
                    if self.os_buffer.is_in_set(par_obj):
                        pass
                    else:
                        self.io.add_io_cost(Const.L_CLRR, Const.L_INSERT, math.ceil(par_obj.count / Const.PARTITION_SIZE))

                    # Add or update partition to buffer
                    os_flushed = self.os_buffer.set(par_obj, (par_obj.count+1) * self.descriptor_size)

                    for j in os_flushed:
                        self.io.add_io_cost(Const.L_CLRW, Const.L_INSERT, math.ceil(j / self.size_target))
                self.ins_buffer.clear_values()

            # Then we account for the cost of partition files, if needed
            if self.repro_method == Const.REPRO_PFILES:
                if self.flush_method == Const.FLUSH_SCAN:
                    # Either a full scan of the partition files
                    num_pages = self.get_total_pfiles_size()
                    self.io.add_io_cost(Const.L_CLSR, Const.L_INSERT,
                                        num_pages)
                    self.io.add_io_cost(Const.L_CLSW, Const.L_INSERT,
                                        num_pages)
                    self.ins_buffer.clear_leaves()
                else:
                    # Or a read and write of partition files in ins_buffer
                    # Only need to read and write the end of the files

                    #get affected partitions
                    par_list = self.ins_buffer.return_leaves()

                    for par in par_list:
                        par_obj = self.leaves[par]
                        # check if in OS buffer
                        if self.os_buffer.is_in_set(par_obj):
                            pass
                        else:
                            self.io.add_io_cost(Const.L_CLRR, Const.L_INSERT, 1)

                        # Add or update partition file to buffer (in bytes: count of features * their size)
                        os_flushed = self.os_buffer.set(par_obj, par_obj.count * self.feature_size)

                        for j in os_flushed:
                            self.io.add_io_cost(Const.L_CLRW, Const.L_INSERT, math.ceil(j / self.size_target))
                    self.ins_buffer.clear_leaves()

        # Finally, do the insertion to the tree and partition
        self.size += 1
        if self.leaves[identifier].insert() == Const.FULL:
            self.split(identifier)

    def get_index_stats(self):
        buffers = self.ins_buffer
        part_in_buffers = len(buffers.ValuePairDict)
        part_in_tree = len(self.leaves)
        nodes_in_tree = len(self.nodes)
        pfiles_in_buffers = len(buffers.FeatureDict)
        pfiles_in_tree = self.get_total_pfiles_count()
        pfiles_size = self.get_total_pfiles_size()
        return [
            '{0:s}\t{1:d}'.format("Tree_Size", self.size),
            '{0:s}\t{1:f}'.format("Tree_Size_Call", self.get_size()),
            '{0:s}\t{1:f}'.format("Tree_Utilization", self.get_util()),
            '{0:s}\t{1:f}'.format("Tree_Max_Leaves", self.max_leaves),
            '{0:s}\t{1:d}'.format("Tree_Partitions_Count", part_in_tree),
            '{0:s}\t{1:d}'.format("Tree_Nodes_Count", nodes_in_tree),
            '{0:s}\t{1:d}'.format("Tree_Pfiles_Count", pfiles_in_tree),
            '{0:s}\t{1:d}'.format("Tree_Pfiles_Size", pfiles_size),
            '{0:s}\t{1:d}'.format("Tree_Split_Count", self.split_count),
            '{0:s}\t{1:d}'.format("Buffer_Flush_Count",
                                  buffers.TotalFlushCount),
            '{0:s}\t{1:d}'.format("Buffer_Pairs_Count",
                                  buffers.TotalValuePairCount),
            '{0:s}\t{1:d}'.format("Buffer_Features_Count",
                                  buffers.TotalFeatureCount),
            '{0:s}\t{1:d}'.format("Buffer_Partitions_Count", part_in_buffers),
            '{0:s}\t{1:d}'.format("Buffer_Pfiles_Count", pfiles_in_buffers)
        ]

    # Used to clear buffer with when simulation ends.
    def clear_osbuf(self):
        if self.buffer_size > 0:
            os_flushed = self.os_buffer.flush_buffer()
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.C_FLUSH, math.ceil(j / self.size_target))
        else:
            pass

    # ----- SPECIFIC INDEX METHODS -----

    # Get a new, unused identifier
    def get_new_identifier(self):
        return len(self.leaves)

    # Calculates the total utilization for the tree
    def print_util(self):
        for i in self.leaves:
            util = 1.0 * i.count / Const.PARTITION_SIZE
            print("Partition: {}, C: {}, U: {}, N: {}, P: {}".format(i.identifier, i.count, util, len(i.parent_node.partitions), i.parent_node))

    # Calculates the total utilization for the tree
    def get_util(self):
        total = 0
        for i in self.leaves:
            total += i.count
        return 1.0 * total / (len(self.leaves) * Const.PARTITION_SIZE)

    # Total tree size (value pairs)
    def get_size(self):
        s = 0
        for i in self.leaves:
            s += i.count
        return s

    # Implements formula 11 of section 5.2.2, but without overlap
    @staticmethod
    def get_total_node_size(node):
        count = 0
        for p in node.partitions:
            count += p.count
        return count

    # Get a partition to insert into, assuming no overlap
    def partition_to_insert(self):
        # Find the partition to insert into
        while True:
            # Choose a random partition
            rn = int(random.random() * len(self.leaves))

            # Compare to the probability of choosing that one
            # If not selected, the loop will try another one
            if self.leaves[rn].prob == 1.0:
                return rn
            if random.random() < self.leaves[rn].prob:
                return rn

    def features_to_pages(self, count):
        return int(math.ceil(
            1.0 * count * self.feature_size / Const.PARTITION_SIZE))

    def get_total_pfiles_size(self):
        # If partition files are not used, size is 0
        if self.repro_method != Const.REPRO_PFILES:
            return 0

        # Else compute total size of partition files
        res = 0
        for n in self.nodes:
            res += self.features_to_pages(self.get_total_node_size(n))
        return res

    def get_total_pfiles_count(self):
        if self.repro_method != Const.REPRO_PFILES:
            return 0
        if self.split_method == Const.LEAF_SPLIT:
            return len(self.leaves)
        else:
            return len(self.nodes)

    # A proxy for the actual split algorithms
    def split(self, identifier):
        self.split_count += 1

        if self.split_method == Const.PARENT_SPLIT:
            self.parent_split(identifier)
        elif self.split_method == Const.HYBRID_SPLIT:
            self.hybrid_split(identifier)

    # The hybrid split algorithm
    def hybrid_split(self, identifier):
        parent_node = self.leaves[identifier].parent_node

        # If the width did not hit max_leaves, then this is simply parent
        # Note that size the simulator only does "leaf splits" AFTER
        # the size is larger, we must multiply by utilization
        if len(parent_node.partitions) < self.max_leaves * Const.UTILIZATION:
            self.parent_split(identifier)
            return

        # Otherwise, we must split the node into four nodes.
        # Then we simply split the partition in question using hybrid split
        num_features = self.get_total_node_size(parent_node)

        # If needed: Cost of maintaining the partition file
        if self.repro_method == Const.REPRO_PFILES:
            # Read the old file
            scan_pages = self.features_to_pages(num_features)
            self.io.add_io_cost(Const.L_CLSR, Const.L_SPLIT, scan_pages)

        # Create the parent nodes (existing node is the first one)
        new_nodes = [None for i in range(Const.LEAF_SPLIT_NODES)]
        new_nodes[0] = parent_node
        for i in range(1, Const.LEAF_SPLIT_NODES):
            new_nodes[i] = PartitionNode.PartitionNode()
            self.nodes.append(new_nodes[i])

        # Make a copy of the partitions and clear the existing node
        partitions = parent_node.partitions
        parent_node.partitions = []

        # Then, select a new parent for all the partitions round-robin
        i = 0
        for p in partitions:
            # Append the partition to round-robin node
            p.parent_node = new_nodes[i % Const.LEAF_SPLIT_NODES]
            p.parent_node.add_partition(p)
            i += 1

        # If needed: Cost of maintaining the partition file
        if self.repro_method == Const.REPRO_PFILES:
            # Write the new files
            for n in new_nodes:
                scan_pages = \
                    self.features_to_pages(self.get_total_node_size(n))
                self.io.add_io_cost(Const.L_CLSW, Const.L_SPLIT, scan_pages)

        # Now, simply parent split the leaf in question
        self.parent_split(identifier, True)

    # The parent split algorithm
    def parent_split(self, identifier, add_depth=False):
        # Step 1: Maintain the partitions themselves
        # Find the parent node and current number of partitions
        parent_node = self.leaves[identifier].parent_node
        old_part = len(parent_node.partitions)

        # Find the number of partitions after the split
        # by dividing number of unique values onto non-overlapping partitions
        # and then using computing the number of overlapping partitions
        num_pairs = self.get_total_node_size(parent_node)
        new_part = int(math.ceil(
            num_pairs / (Const.PARTITION_SIZE * Const.UTILIZATION)))

        # Calculate how many value pairs go to each partition
        # This corresponds to formula 13, but computed a bit differently
        new_count = int(math.ceil(1.0 * num_pairs / new_part))

        # Calculate the new probability of insertion
        # This corresponds to formula
        old_total_prob = 0.0
        for p in parent_node.partitions:
            old_total_prob += p.prob
        old_avg_prob = old_total_prob / len(parent_node.partitions)
        new_prob = old_avg_prob * old_part / new_part

        # Update the old partitions with value pair count and probability
        for p in parent_node.partitions:
            p.prob = new_prob
            p.count = new_count

        # Add new partitions as needed
        for i in range(0, new_part - old_part):
            p = Partition.Partition(self.get_new_identifier(),
                                    new_count, new_prob, parent_node)
            parent_node.add_partition(p)
            self.leaves.append(p)

        # Clear all the data for these partitions from buffers
        for p in parent_node.partitions:
            self.ins_buffer.delete(p.identifier)

        # Step 2: Cost of the split (no change in the partition files)
        # for each new (split) partition
        for par in parent_node.partitions:
            #check if in os buffer (if not - like the new partitions - add cost of read)
            if self.os_buffer.is_in_set(par):
                pass
            else:
                self.io.add_io_cost(Const.L_CLRR, Const.L_SPLIT, 1)

            # Add or update partition to LRU buffer
            os_flushed = self.os_buffer.set(par, par.count * self.feature_size)

            # Add IO cost of LRU flushed items.
            for j in os_flushed:
                self.io.add_io_cost(Const.L_CLRW, Const.L_SPLIT, math.ceil(j / self.size_target))

        # Step 3: IO cost of the re-projection
        if self.repro_method == Const.REPRO_PFILES:
            # If we are splitting one partition, we just maintained it
            # so it will be in memory
            if not add_depth:
                # Read only the relevant partition file
                scan_pages = self.features_to_pages(num_pairs)
                self.io.add_io_cost(Const.L_CLSR, Const.L_REPRO, scan_pages)
        elif self.repro_method == Const.REPRO_SCAN:
            # Scan the entire feature collection
            scan_pages = self.features_to_pages(self.size)
            self.io.add_io_cost(Const.L_CLSR, Const.L_REPRO, scan_pages)
        else:
            # Randomly read the features needed (based on identifiers)
            self.io.add_io_cost(Const.L_CFRR, Const.L_REPRO, num_pairs)