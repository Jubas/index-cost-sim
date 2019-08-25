import Const
import math


# Partition node is the parent of leaf partitions.
# Used for hybrid and parent splitting operations.
class PartitionNode:
    def __init__(self):
        # Start with an empty array of leaves
        self.partitions = []

    # Adds a new leaf to the partition node
    def add_partition(self, partition):
        self.partitions.append(partition)

    def __str__(self):
        out_string = "Node: " + \
                     "P: " + str(len(self.partitions)) + "\n"
        for p in self.partitions:
            out_string += "  " + p.__str__() + "\n"
        return out_string
