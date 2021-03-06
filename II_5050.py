import Const
import RunClassCommon

OPT900P_costs = [0.0593, 0.0653, 0.0584, 0.0659, 0.0076, 0.0117, 0.0076, 0.0287]
OPTMem_costs = [0.1001, 0.4416, 0.0997, 0.4429, 0.0057, 0.0093, 0.0060, 0.0177]

experiment_list = [
    {
    'Experiment': Const.INV_IDX,
    'Setup': 'II_5050',
    'Flush_Method': Const.FLUSH_READ,
    'Ins_Buffer_Size': 34359738368,
    'OS_Buffer_Size': 34359738368,
    'IO_Costs': OPT900P_costs,
    'Write_Iotrace': True,
    'Log_Name': 'II_5050_Trace.log'
    }
]

if __name__ == '__main__':
    RunClassCommon.main(experiment_list)
