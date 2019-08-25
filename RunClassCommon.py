import Const
import Simulation


def run_simulation(sim_dict):
    # --- GENERIC DEFAULT PARAMS ---
    # The "experiment" measured -- group of graphs
    if 'Experiment' not in sim_dict:
        sim_dict['Experiment'] = 'No_Name_Exp'
    # The "setup" measured -- line in a graph
    if 'Setup' not in sim_dict:
        sim_dict['Setup'] = 'No_Name_Setup'
    # The cost of IOs
    if 'IO_Costs' not in sim_dict:
        sim_dict['IO_Costs'] = [7.5, 7.5, 0.75, 0.75]
    # Initial descriptors in index
    if 'Initial_Size' not in sim_dict:
        sim_dict['Initial_Size'] = 50000000
    # Length of the simulation
    if 'Total_Inserts' not in sim_dict:
        sim_dict['Total_Inserts'] = 1500000000
    # Frequency of output -- every X queries
    if 'Output_Freq' not in sim_dict:
        sim_dict['Output_Freq'] = 10000000
    # Defines the optimal size of an IO
    if 'IO_Size' not in sim_dict:
        sim_dict['IO_Size'] = Const.IO_SIZE
    # The size of descriptors (bytes)
    if 'Descriptor_Size' not in sim_dict:
        sim_dict['Descriptor_Size'] = Const.DESCRIPTOR_SIZE
    # The memory buffer size (default = 4GB, 0 = OFF)
    if 'Buffer_Size' not in sim_dict:
        sim_dict['Buffer_Size'] = Const.BUFFER_64GB

    if 'Ins_Buffer_Size' not in sim_dict:
        sim_dict['Ins_Buffer_Size'] = Const.INS_BUFFER_SIZE

    if 'OS_Buffer_Size' not in sim_dict:
        sim_dict['OS_Buffer_Size'] = Const.OS_BUFFER_SIZE

    if 'OS_Frame_size' not in sim_dict:
        sim_dict['OS_Frame_Size'] = Const.OS_FRAME_SIZE

    # The buffer flush method (default = "scan index / partition files")
    if 'Flush_Method' not in sim_dict:
        sim_dict['Flush_Method'] = Const.FLUSH_SCAN

    if 'Write_Iotrace' not in sim_dict:
        sim_dict['Write_Iotrace'] = Const.WRITE_IOTRACE

    if 'Log_Name' not in sim_dict:
        sim_dict['Log_Name'] = Const.LOG_NAME

    # --- NVTREE DEFAULT PARAMS ---
    if sim_dict['Experiment'] == Const.NVTREE:
        # The split method (default = "hybrid")
        if 'Split_Method' not in sim_dict:
            sim_dict['Split_Method'] = Const.HYBRID_SPLIT
        # The re-projection method (default = "scan collection")
        if 'Repro_Method' not in sim_dict:
            sim_dict['Repro_Method'] = Const.REPRO_SCAN

        # max_leaves for tree
        if 'Max_Leaves' not in sim_dict:
            sim_dict['Max_Leaves'] = 36

    # --- eCP DEFAULT PARAMS ---
    elif sim_dict['Experiment'] == Const.EXT_CP:

        # The amount of added leaders to use for reclustering
        if 'Recluster_Pct' not in sim_dict:
            sim_dict['Recluster_Pct'] = Const.RECLUSTER_PERCENT

        # The cluster size strategy
        if 'Cluster_Size_Strategy' not in sim_dict:
            sim_dict['Cluster_Size_Strategy'] = Const.SIZE_ABSOLUTE
        # The leader size strategy
        if 'Leader_Size_Strategy' not in sim_dict:
            sim_dict['Leader_Size_Strategy'] = Const.LEAD_SIZE_ABSOLUTE

        # The target leader size (used initially and when resizing)
        if 'Leader_Target_Size' not in sim_dict:
            sim_dict['Leader_Target_Size'] = Const.LEAD_TARGET_SIZE
        # The lowerbound we wish to have from target when resizing leaders
        if 'LO_Size_Pct' not in sim_dict or sim_dict['LO_Size_Pct'] < 0:
            sim_dict['LO_Size_Pct'] = Const.LO_SIZE_PCT
        # The highest bound for leader capacity from target
        if 'HI_Size_Pct' not in sim_dict or sim_dict['HI_Size_Pct'] < 0:
            sim_dict['HI_Size_Pct'] = Const.HI_SIZE_PCT

    # --- II DEFAULT PARAMS ---
    elif sim_dict['Experiment'] == Const.INV_IDX:
        # The lowerbound we wish to have from target when resizing subregions
        if 'LO_Size_Pct' not in sim_dict or sim_dict['LO_Size_Pct'] < 0:
            sim_dict['LO_Size_Pct'] = Const.LO_SIZE_PCT
        # The highest bound for subregion capacity from target
        if 'HI_Size_Pct' not in sim_dict or sim_dict['HI_Size_Pct'] < 0:
            sim_dict['HI_Size_Pct'] = Const.HI_SIZE_PCT

        # The amount of PQ codebooks (m)
        if 'M_slices' not in sim_dict or sim_dict['M_slices'] < 0:
            sim_dict['M_slices'] = Const.M_SLICES
        # The amount of nearest neighbor clusters for some cluster 'c' to maintain subregions for
        if 'NNL_c' not in sim_dict or sim_dict['NNL_c'] < 0:
            sim_dict['NNL_c'] = Const.NNL_C

        if 'K_rounds' not in sim_dict:
            sim_dict['K_rounds'] = Const.K_ROUNDS
    else:
        raise NotImplementedError("Add new simulation defaults for undefined index type")

    # Initialize simulation and run it
    s = Simulation.Simulation(sim_dict)
    s.simulate()


def main(exp_list):
    for i in range(0, len(exp_list)):
        run_simulation(exp_list[i])
