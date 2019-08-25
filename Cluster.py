#imports

class Cluster():
    def __init__(self, count=1, leader=None):
        # count of descriptors 
        # (default is 1, as a leader is always in the cluster on instantiation)
        self.count = count
        # cluster leader
        self.leader = leader

    def insert(self):
        self.count = self.count + 1

    def __str__(self):
        return "Cluster: " + \
               "ID: " + str(self.identifier) + ";" + \
               "C: " + str(self.count)
