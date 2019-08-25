import Const
import math
import random
import os

## TODO: IOLOGGING
class IO:
    def __init__(self, experiment, setup):
        self.Experiment = experiment
        self.IOCostParamDict = {}
        self.TotalIOCostDict = {}
        self.TotalIOCountDict = {}
        self.Write_IOtrace = setup['Write_Iotrace']
        self.Log_Name = setup['Log_Name']
        self.IO_Size = setup['IO_Size']
        self.Descriptor_Size = setup['Descriptor_Size']

        if self.Write_IOtrace == True:
            self.IODepthDict = {}
            for i in range(Const.L_IO_TYPES):
                self.IODepthDict[i] = []
                for j in Const.IO_DEPTHS:
                    self.IODepthDict[i].append(0)

        for i in range(0, Const.L_IO_TYPES):
            self.IOCostParamDict[i] = 0.0
            if self.Experiment == Const.NVTREE:
                for j in range(0, Const.L_IO_SOURCES):
                    self.TotalIOCostDict[(i, j)] = 0.0
                    self.TotalIOCountDict[(i, j)] = 0
            elif self.Experiment == Const.EXT_CP:
                for j in range(0, Const.C_IO_SOURCES):
                    self.TotalIOCostDict[(i, j)] = 0.0
                    self.TotalIOCountDict[(i, j)] = 0
            elif self.Experiment == Const.INV_IDX:
                for j in range(0, Const.C_IO_SOURCES):
                    self.TotalIOCostDict[(i, j)] = 0.0
                    self.TotalIOCountDict[(i, j)] = 0
            else:
                raise NotImplementedError("Add new simulation for undefined index type")

    def set_io_cost_params(self,
                           clrr, clrw,
                           clsr, clsw,
                           cfrr=0.0, cfrw=0.0,
                           cfsr=0.0, cfsw=0.0):
        self.IOCostParamDict[Const.L_CLRR] = clrr
        self.IOCostParamDict[Const.L_CLRW] = clrw
        self.IOCostParamDict[Const.L_CLSR] = clsr
        self.IOCostParamDict[Const.L_CLSW] = clsw
        self.IOCostParamDict[Const.L_CFRR] = cfrr
        self.IOCostParamDict[Const.L_CFRW] = cfrw
        self.IOCostParamDict[Const.L_CFSR] = cfsr
        self.IOCostParamDict[Const.L_CFSW] = cfsw

    def set_io_cost_param_array(self, cost_param_array):
        for i in range(0, len(cost_param_array)):
            self.IOCostParamDict[i] = cost_param_array[i]

    def write_to_iodepth(self, io_type, count):
        if count > 0:
            for idx, x in enumerate(Const.IO_DEPTHS):
                if (count / int(x)) <= 1.0:
                    self.IODepthDict[io_type][idx] += 1
                    break
            # If count gt than greatest value in IO Depths
            if count > int(Const.IO_DEPTHS[len(Const.IO_DEPTHS) -1]):
                self.IODepthDict[io_type][len(Const.IO_DEPTHS) -1] += 1
        else:
            return

    def add_io_cost(self, io_type, io_source, count=1):
        if self.Write_IOtrace == True:
            self.write_to_iodepth(io_type, count)
        self.TotalIOCountDict[(io_type, io_source)] += count
        self.TotalIOCostDict[(io_type, io_source)] += \
            self.IOCostParamDict[io_type] * count

    def get_io_cost(self, io_type, io_source=None):
        if io_source is not None:
            return self.TotalIOCostDict[(io_type, io_source)] / 3600000.0
        else:
            total_io_cost = 0.0
            if self.Experiment == Const.NVTREE:
                for i in range(0, Const.L_IO_SOURCES):
                    total_io_cost += self.TotalIOCostDict[(io_type, i)]
            elif self.Experiment == Const.EXT_CP:
                for i in range(0, Const.C_IO_SOURCES):
                    total_io_cost += self.TotalIOCostDict[(io_type, i)]
            elif self.Experiment == Const.INV_IDX:
                for i in range(0, Const.C_IO_SOURCES):
                    total_io_cost += self.TotalIOCostDict[(io_type, i)]
            else:
                raise NotImplementedError("Add new simulation for undefined index type")

            # Convert cost to hours before returning
            return total_io_cost / 3600000.0

    def get_io_count(self, io_type, io_source=None):
        if io_source is not None:
            return self.TotalIOCountDict[(io_type, io_source)]
        else:
            total_io_count = 0
            if self.Experiment == Const.NVTREE:
                for i in range(0, Const.L_IO_SOURCES):
                    total_io_count += self.TotalIOCountDict[(io_type, i)]
            elif self.Experiment == Const.EXT_CP:
                for i in range(0, Const.C_IO_SOURCES):
                    total_io_count += self.TotalIOCountDict[(io_type, i)]
            elif self.Experiment == Const.INV_IDX:
                for i in range(0, Const.C_IO_SOURCES):
                    total_io_count += self.TotalIOCountDict[(io_type, i)]
            else:
                raise NotImplementedError("Add new simulation for undefined index type")

            return total_io_count

    def get_io_stat(self, io_type=None, io_source=None):
        if io_type is not None:
            if io_source is not None:
                if self.Experiment == Const.NVTREE:
                    count_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.L_IO_SOURCE_NAMES[io_source] + "_Count"
                    cost_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.L_IO_SOURCE_NAMES[io_source] + "_Cost"
                elif self.Experiment == Const.EXT_CP:
                    count_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.C_IO_SOURCE_NAMES[io_source] + "_Count"
                    cost_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.C_IO_SOURCE_NAMES[io_source] + "_Cost"
                elif self.Experiment == Const.INV_IDX:
                    count_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.C_IO_SOURCE_NAMES[io_source] + "_Count"
                    cost_str = \
                        Const.L_IO_TYPE_NAMES[io_type] + "_" + \
                        Const.C_IO_SOURCE_NAMES[io_source] + "_Cost"
                else:
                    raise NotImplementedError("Add new simulation for undefined index type")
            else:
                count_str = Const.L_IO_TYPE_NAMES[io_type] + "_Total_Count"
                cost_str = Const.L_IO_TYPE_NAMES[io_type] + "_Total_Cost"
            cost = self.get_io_cost(io_type, io_source)
            count = self.get_io_count(io_type, io_source)
            if count == 0:
                return None
            else:
                return ('IO_{0:s}\t{1:d}'.format(count_str, int(count)),
                        'IO_{0:s}\t{1:f}'.format(cost_str, cost))
        else:
            count_str = "Total_Total_Count"
            cost_str = "Total_Total_Cost"

            total_count = 0
            total_cost = 0.0
            for i in range(0, Const.L_IO_TYPES):
                total_count += self.get_io_count(i, None)
                total_cost += self.get_io_cost(i, None)
            if total_count == 0:
                return None
            else:
                return ('IO_{0:s}\t{1:d}'.format(count_str, int(total_count)),
                        'IO_{0:s}\t{1:f}'.format(cost_str, total_cost))

    def get_io_stats(self):
        out_stats = []

        # Get the total IO cost
        str_pair = self.get_io_stat()
        if str_pair is not None:
            out_stats.append(str_pair[0])
            out_stats.append(str_pair[1])

        for i in range(0, Const.L_IO_TYPES):
            # Get the total cost of the IO type
            str_pair = self.get_io_stat(i)
            if str_pair is not None:
                out_stats.append(str_pair[0])
                out_stats.append(str_pair[1])
            if self.Experiment == Const.NVTREE:
                for j in range(0, Const.L_IO_SOURCES):
                    # Get the cost of the IO type from each IO source
                    str_pair = self.get_io_stat(i, j)
                    if str_pair is not None:
                        out_stats.append(str_pair[0])
                        out_stats.append(str_pair[1])
            elif self.Experiment == Const.EXT_CP:
                for j in range(0, Const.C_IO_SOURCES):
                    # Get the cost of the IO type from each IO source
                    str_pair = self.get_io_stat(i, j)
                    if str_pair is not None:
                        out_stats.append(str_pair[0])
                        out_stats.append(str_pair[1])
            elif self.Experiment == Const.INV_IDX:
                for j in range(0, Const.C_IO_SOURCES):
                    # Get the cost of the IO type from each IO source
                    str_pair = self.get_io_stat(i, j)
                    if str_pair is not None:
                        out_stats.append(str_pair[0])
                        out_stats.append(str_pair[1])
            else:
                raise NotImplementedError("Add new simulation for undefined index type")

        return out_stats
