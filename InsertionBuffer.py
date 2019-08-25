import Const


# The Buffers class keeps track of the insert buffer management
# It does NOT keep track of actual partition sizes and such
# It does bookkeeping of IO costs for inserts, but
class InsertionBuffer:
    def __init__(self, size):
        # Store parameter values
        self.size = size

        # Size for (ID, value) pairs: 1 int + 1 float
        self.ValuePairSize = Const.VALUE_PAIR_SIZE

        # Size for features: DIMENSION bytes + 1 int
        self.FeatureSize = (Const.DIMENSION * 1) + (1 * 4)

        self.value_list = []
        self.feature_list = []

        # Initialize data structures
        self.ValuePairDict = {}
        self.FeatureDict = {}

        # Initialize data structure totals
        self.TotalValuePairCount = 0
        self.TotalFeatureCount = 0
        self.TotalFlushCount = 0

    def bytes_in_buffers(self):
        return self.ValuePairSize * self.TotalValuePairCount + self.FeatureSize * self.TotalFeatureCount

    def is_full(self):
        return self.bytes_in_buffers() >= self.size

    # Insert a value pair to a partition
    # Return True if a flush was needed
    def insert_value_pair(self, identifier, include_feature):
        if identifier in self.ValuePairDict:
            self.ValuePairDict[identifier] += 1
        else:
            self.ValuePairDict[identifier] = 1
        self.TotalValuePairCount += 1

        if include_feature:
            if identifier in self.FeatureDict:
                self.FeatureDict[identifier] += 1
            else:
                self.FeatureDict[identifier] = 1
            self.TotalFeatureCount += 1

        # If buffers overflow, then flush the entire buffers
        if self.is_full():
            num_part = len(self.ValuePairDict)
            num_pfiles = len(self.FeatureDict)
            self.value_list = list(self.ValuePairDict.keys())
            self.id_list = list(self.FeatureDict.keys())
            self.flush()
            return True, num_part, num_pfiles
        else:
            return False, 0, 0

    # Insert a feature to a partition file, but only if there is space
    # Return True if a flush was needed
    def insert_feature(self, identifier):
        if identifier in self.FeatureDict:
            self.FeatureDict[identifier] += 1
        else:
            self.FeatureDict[identifier] = 1
        self.TotalFeatureCount += 1

        # If buffers overflow, then flush the entire buffers
        if self.is_full():
            num_part = len(self.ValuePairDict)
            num_pfiles = len(self.FeatureDict)
            self.value_list = list(self.ValuePairDict.keys())
            self.id_list = list(self.FeatureDict.keys())
            self.flush()
            return True, num_part, num_pfiles
        else:
            return False, 0, 0

    # Insert a feature to a partition file, but only if there is space
    # Return True if a flush was needed
    # Modified to take a leader
    def insert_feature_leader(self, leader):
        identifier = leader.identity
        if identifier in self.FeatureDict:
            self.FeatureDict[identifier] += 1
        else:
            self.FeatureDict[identifier] = 1
        self.TotalFeatureCount += 1

        # If buffers overflow, then flush the entire buffers
        if self.is_full():
            num_part = len(self.ValuePairDict)
            num_pfiles = len(self.FeatureDict)
            self.value_list = list(self.ValuePairDict.keys())
            self.id_list = list(self.FeatureDict.keys())
            self.flush()
            return True, num_part, num_pfiles
        else:
            return False, 0, 0

    #Used to return partition files
    def return_values(self):
        return self.value_list

    def clear_values(self):
        self.value_list = []

    # Used to return leaves (the leaders of Clusters), because these may be variable size (unlike NVtree Partitions)
    # And we can therefore not rely on 'num_part'/'_pfiles'
    def return_leaves(self):
        return self.feature_list

    def clear_leaves(self):
        self.feature_list = []

    # Delete partition, both value pairs and features if applicable,
    # also in lists
    def delete(self, identifier):
        if identifier in self.FeatureDict:
            self.TotalFeatureCount -= self.FeatureDict[identifier]
            del self.FeatureDict[identifier]
        if identifier in self.ValuePairDict:
            self.TotalValuePairCount -= self.ValuePairDict[identifier]
            del self.ValuePairDict[identifier]

    def flush(self):
        # IO cost will be managed elsewhere
        # Empty all partitions and reset counters
        self.TotalFlushCount += 1
        self.TotalFeatureCount = 0
        self.TotalValuePairCount = 0
        self.FeatureDict = {}
        self.ValuePairDict = {}
