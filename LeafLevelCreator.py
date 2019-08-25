# System import
import math

# user import
import Partition
import PartitionNode
import Const


# Implements the formulas of section 3
class LeafLevelCreator:
    def __init__(self, size, max_leaves):
        # Store parameters
        self.size = size
        self.max_leaves = max_leaves

    # Create all leaves for the tree
    def create_leaves_and_nodes(self):
        # Compute desired fill factors
        features_per_partition = int(Const.PARTITION_SIZE * Const.UTILIZATION)
        partitions_per_node = int(self.max_leaves * Const.UTILIZATION)

        # Compute number of partitions and nodes
        partitions = int(math.ceil(self.size / features_per_partition))

        # Create all the partitions and partition nodes
        nodes = []
        leaves = []
        for i in range(0, partitions):
            if i % partitions_per_node == 0:
                node = PartitionNode.PartitionNode()
                nodes.append(node)
            leaf = Partition.Partition(i, features_per_partition, 1.0, node)
            node.add_partition(leaf)
            leaves.append(leaf)
        return leaves, nodes
