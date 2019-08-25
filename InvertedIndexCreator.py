#Imports
import math
import random

import Const

'''
NOTE: 
Relies on numpy.
'''
class InvertedIndexCreator:
    def __init__(self, size, target_subregion_size, lo_pct, hi_pct, lamb=2, L=5):
        #params
        self.size = size
        self.target_subregion_size = target_subregion_size
        self.lo_pct = lo_pct
        self.hi_pct = hi_pct
        self.lamb = lamb
        self.L = L

    '''
    Gives the number of k regions in the coarse quantizer (qc).
    '''
    def calculate_k(self):
        #Adjust target subregion sizes for 'lo'
        adjusted_subregion_size = math.ceil((self.target_subregion_size/100)*(100-self.lo_pct))

        # pro_region = gives the target size for a region, given 'L' subregions * adjusted_subregion_size
        self.pro_region = self.L * adjusted_subregion_size

        #calculate 'k' - or rather, the number of codes/regions we need.
        k = math.ceil(self.size/self.pro_region)
        return k

    def create_structure(self, k):
        coarse_quantizer = []
        inv_idx = []
        size = 1 + self.L
        total_size = size * k
        for i in range(k):

            L_list = []
            for i in range(self.L):
                L_list.append(1)

            inv_idx.append(L_list)
            coarse_quantizer.append(size)
        return coarse_quantizer, inv_idx, total_size

    def create_index(self):
        # find k size
        self.k = self.calculate_k()

        qc, invidx, inserted = self.create_structure(self.k)

        to_insert = self.size - inserted

        # insert loop
        for i in range(to_insert):
            #y = random.expovariate(self.lamb)
            #while (0 <= y < 1) == False:
            #    y = random.expovariate(self.lamb)

            #idx = int(len(qc) * y)
            idx = int(len(qc) * random.random())

            # retrieve L subregions
            subregions = invidx[idx]

            # choose random (uniform) subregion idx to insert into
            s_idx = int(len(subregions) * random.random())

            invidx[idx][s_idx] += 1
            qc[idx] += 1
        return qc, invidx