import Const
import RunClassCommon

OPT900P_costs = [0.0593, 0.0653, 0.0584, 0.0659, 0.0076, 0.0117, 0.0076, 0.0287]
OPTMem_costs = [0.1001, 0.4416, 0.0997, 0.4429, 0.0057, 0.0093, 0.0060, 0.0177]
RAMDISK = [0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005, 0.0005]

experiment_list = [
    {
    'Experiment': Const.EXT_CP,
    'Setup': 'RAMDISK_ECP_ClusterAbs_LeadAbs_Read',
    'Flush_Method': Const.FLUSH_READ,
    'Recluster_Pct': 20,
    'Ins_Buffer_Size': 17179869184,
    'OS_Buffer_Size': 51539607552,
    'Cluster_Size_Strategy': Const.SIZE_ABSOLUTE,
    'Leader_Size_Strategy': Const.LEAD_SIZE_ABSOLUTE,
    'Leader_Target_Size': 100,
    'IO_Costs': RAMDISK,
    'Write_Iotrace': True,
    'Log_Name': 'RAMDISK_ECP_ClusterAbs_LeadAbs_Read.log'
    }
]

if __name__ == '__main__':
    RunClassCommon.main(experiment_list)
