import math
import random

import InvertedIndexCreator
import Const
import InsertionBuffer
import OSBuffer

class InvIdx:
    def __init__(self, io, setup):
        #read setup params
        self.io = io
        self.size = setup['Initial_Size']
        self.size_target = setup['IO_Size']
        self.flush_method = setup['Flush_Method']
        self.buffer_size = setup['Buffer_Size']
        self.ins_buffer_size = setup['Ins_Buffer_Size']
        self.os_buffer_size = setup['OS_Buffer_Size']
        self.os_frame_size = setup['OS_Frame_Size']

        self.lo_pct = setup['LO_Size_Pct']
        self.hi_pct = setup['HI_Size_Pct']
        self.lamb = 447.25
        self.m = setup['M_slices']
        self.L = setup['NNL_c']
        self.K = setup['K_rounds']

        # denotes the size of a id ref of descriptor + PQ-compressed descriptor (with m slices)
        # 1 int (4bytes) for ref, 1 int per 'm'
        self.comp_desc_size = (1 * 4) + (4 * self.m)

        # floor(io size / compressed descriptor size)
        ## denotes the amount of comp. descriptors per subregion in our optimal scenario
        self.target_subregion_size = math.floor(self.size_target / self.comp_desc_size)

        # Create the buffer manager
        if self.buffer_size != 0:
            self.ins_buffer = InsertionBuffer.InsertionBuffer(self.ins_buffer_size)
            self.os_buffer = OSBuffer.OSBuffer(self.os_buffer_size, self.os_frame_size)

        self.iic = InvertedIndexCreator.InvertedIndexCreator(
                                                            self.size,
                                                            self.target_subregion_size,
                                                            self.lo_pct,
                                                            self.hi_pct,
                                                            self.lamb,
                                                            self.L
                                                            )

        #create init clustering
        self.qc, self.invidx = self.iic.create_index()

    # ----- GENERIC INTERFACE METHODS -----
    def insert(self):
        # choose random (exponential distribution) region to insert into
        #y = random.expovariate(self.lamb)
        #while (0 <= y < 1) == False:
        #    y = random.expovariate(self.lamb)

        #idx = int(len(self.qc) * y)

        idx = int(len(self.qc) * random.random())

        subregions = self.invidx[idx]

        # choose random (uniform) subregion idx to insert into
        s_idx = int(len(subregions) * random.random())
        chosen_sub = subregions[s_idx]
        org_size = math.ceil(chosen_sub/self.target_subregion_size)

        triple = (False, 0, 0)

        # No size policies (dont work for skewed distributions), 
        # check if average region size is bigger than target subregion size * L (essentially target avg region size)
        if self.check_avg_region_size():
            self.recluster_index()
        else:
            # do IO book-keeping for insert
            if self.buffer_size == 0:
                # Direct ins
                # Account for org read cost
                self.io.add_io_cost(Const.L_CLSR, Const.C_INSERT, org_size)
                # Account for new size
                self.io.add_io_cost(Const.L_CLSW, Const.C_INSERT, math.ceil((subregions[s_idx]+1)/self.target_subregion_size))
            else:
                # buf ins (uses region+subregion idx tuple, as all subregions are under the region)
                triple = self.ins_buffer.insert_feature((idx, s_idx))

        # if we have a buf mgr and we flushed
        if self.buffer_size > 0:
            if triple[0]:
                # Full scan cost
                if self.flush_method == Const.FLUSH_SCAN:
                    #for each region
                    for i in range(len(self.invidx)):
                        # for each subregion
                        for j in range(len(self.invidx[i])):
                            #check if subregion in OS buffer
                            if self.os_buffer.is_in_set((i,j)):
                                pass
                            else:
                                # if not in buffer, add read cost
                                self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, math.ceil(self.invidx[i][j]/self.target_subregion_size))
                            
                            # Add or update subregion to buffer
                            os_flushed = self.os_buffer.set((i,j), self.invidx[i][j] * self.comp_desc_size)

                            # Add write cost for any potential flushed regions
                            for k in os_flushed:
                                self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(k / self.size_target))

                # Cost of flush of modified subregions
                else:
                    region_pairs = self.ins_buffer.return_leaves()

                    #for each in (idx, s_idx) pair
                    for i in region_pairs:
                        if self.os_buffer.is_in_set((i[0], i[1])):
                            pass
                        else:
                            # if not in buffer, add read cost
                            self.io.add_io_cost(Const.L_CLRR, Const.C_INSERT, math.ceil(self.invidx[i[0]][i[1]]/self.target_subregion_size))
                        
                        # Add or update subregion to buffer
                        os_flushed = self.os_buffer.set((i[0], i[1]), self.invidx[i[0]][i[1]] * self.comp_desc_size)

                        # Add write cost for any potential flushed regions
                        for j in os_flushed:
                            self.io.add_io_cost(Const.L_CLRW, Const.C_INSERT, math.ceil(j / self.size_target))

                    self.ins_buffer.clear_leaves()

        # Insertions
        self.invidx[idx][s_idx] += 1
        self.qc[idx] += 1
        self.size += 1

    def get_index_stats(self):
        util = self.get_util()
        min_r, max_r, avg_r = self.get_region_stats()
        min_s, max_s, avg_s = self.get_subregion_stats()

        return [
            '{0:s}\t{1:d}'.format("II_Size", self.size),
            '{0:s}\t{1:d}'.format("II_K", len(self.qc)),
            '{0:s}\t{1:f}'.format("II_Utilization", util),
            '{0:s}\t{1:d}'.format("Min_Region_Size", min_r),
            '{0:s}\t{1:d}'.format("Max_Region_Size", max_r),
            '{0:s}\t{1:f}'.format("Avg_Region_Size", avg_r),
            '{0:s}\t{1:d}'.format("Min_Subregion_Size", min_s),
            '{0:s}\t{1:d}'.format("Max_Subregion_Size", max_s),
            '{0:s}\t{1:f}'.format("Avg_Subregion_Size", avg_s)
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
    def get_util(self):
        total = 0
        subregion_amount = 0
        for i in self.invidx:
            for j in i:
                total += j
            subregion_amount += len(i)
        return 1.0 * total / (subregion_amount * self.target_subregion_size)

    def get_region_stats(self):
        rs = sorted(self.qc, reverse=True)
        max_r = rs[0]
        min_r = rs[len(rs)-1]
        sum_r = sum(rs)

        avg_r = 1.0 * sum_r / len(rs)
        return min_r, max_r, avg_r

    def get_subregion_stats(self):
        flat_list = [i for lst in self.invidx for i in lst]
        ss = sorted(flat_list, reverse=True)
        max_s = ss[0]
        min_s = ss[len(ss)-1]
        sum_s = sum(ss)

        avg_s = 1.0 * sum_s / len(ss)
        return min_s, max_s, avg_s

    # ----- SPECIFIC INDEX METHODS -----

    def check_avg_region_size(self):
        num_regions = len(self.qc)

        avg_size = self.size / num_regions

        if avg_size > (self.target_subregion_size * self.L):
            return True
        else:
            return False

    def recluster_index(self):
        self.iic = InvertedIndexCreator.InvertedIndexCreator(
                                                            self.size,
                                                            self.target_subregion_size,
                                                            self.lo_pct,
                                                            self.hi_pct,
                                                            self.lamb,
                                                            self.L
                                                            )

        # Add read cost of entire un-compressed index (scan entire collection; happens K times). 
        # This is essentially an approximation of the cost.
        read_cost = math.ceil(self.size * Const.DESCRIPTOR_SIZE / self.size_target) * self.K
        self.io.add_io_cost(Const.L_CLSR, Const.C_RECLUSTER, read_cost)

        #create new clustering (with new size)
        self.qc, self.invidx = self.iic.create_index()

        # Add write cost of entire index (to store index; happens K times).
        # This is essentially an approximation of the cost.
        for i in self.qc:
            write_cost = math.ceil(i / self.target_subregion_size) * self.K
            self.io.add_io_cost(Const.L_CLRW, Const.C_RECLUSTER, write_cost)

        # if we have a buffer
        if self.buffer_size != 0:
            # delete all impending reads and writes, as we have just done a full read and write of everything
            self.ins_buffer.flush()
            #clear all saved regions pairs from ins_buffer
            self.ins_buffer.clear_leaves()

            # reset OS buffer
            self.os_buffer = OSBuffer.OSBuffer(self.os_buffer_size, self.os_frame_size)
