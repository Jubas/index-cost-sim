import Const


class Partition:
    def __init__(self, identifier, count, prob=1, parent_node=None):
        # Basic info
        self.identifier = identifier
        self.parent_node = parent_node

        # Arrays of 5.1
        self.count = count
        self.prob = prob

    # Insert value pair(s) into this segment and inform of situation
    def insert(self, values=1):
        self.count += values

        if self.count >= Const.PARTITION_SIZE:
            return Const.FULL
        else:
            return Const.NOT_FULL

    def __str__(self):
        return "Partition: " + \
               "ID: " + str(self.identifier) + ";" + \
               "C: " + str(self.count) + ";" + \
               "P: " + str(self.prob)
