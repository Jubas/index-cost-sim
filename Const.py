# General constants
ENABLE = True
DISABLE = False
DEBUG = False

# Experiment types
NVTREE = 0
EXT_CP = 1
INV_IDX = 2

# Override these in experiment files for particular IO and descriptor sizes.
IO_SIZE = 131072
DESCRIPTOR_SIZE = 132
DIMENSION = 128
VALUE_PAIR_SIZE = 6

# Buffer sizes
BUFFER_64GB = 68719476736
BUFFER_OFF = 0
INS_BUFFER_SIZE = 34359738368
OS_BUFFER_SIZE = 34359738368
OS_FRAME_SIZE = 131072

## --------- IOTRACE OPTIONS ---------

WRITE_IOTRACE = False

LOG_NAME = 'IO.fio'

## --------- IO DEFAULTS AND OPTIONS ---------

# IO type constants for logging
L_IO_TYPES = 8
L_IO_TYPE_NAMES = ['CLRR', 'CLRW', 'CLSR', 'CLSW',
                   'CFRR', 'CFRW', 'CFSR', 'CFSW']
L_CLRR = 0   # Cost of random read of IO_SIZE
L_CLRW = 1   # Cost of random write of IO_SIZE
L_CLSR = 2   # Cost of sequential read of IO_SIZE
L_CLSW = 3   # Cost of sequential write of IO_SIZE
L_CFRR = 4   # Cost of random read of descriptor size
L_CFRW = 5   # Cost of random write of descriptor size
L_CFSR = 6   # Cost of sequential read of descriptor size
L_CFSW = 7   # Cost of sequential write of descriptor size

# IODEPTHS
# 10000: above 128 IO depth (extremely large)
IO_DEPTHS = ['1', '2', '4', '8', '16', '32', '64', '128', '10000']

## --------- NVTREE DEFAULTS AND OPTIONS ---------
# Partition size and utilization
PARTITION_SIZE = 682.0
LEAF_SPLIT_NODES = 4
UTILIZATION = 0.66667

# Sources of IOs
L_IO_SOURCES = 3
L_IO_SOURCE_NAMES = ['INSERT', 'SPLIT', 'REPRO']
L_INSERT = 0
L_SPLIT = 1
L_REPRO = 2

# Split methods
SPLIT_METHOD_NAMES = ['Leaf', 'Parent', 'Hybrid', 'Regen', 'NoSplit']
LEAF_SPLIT = 0
PARENT_SPLIT = 1
HYBRID_SPLIT = 2

# Re-projection methods
REPRO_METHOD_NAMES = ['Scan', 'Read', 'PartitionFiles']
REPRO_SCAN = 0
REPRO_READ = 1
REPRO_PFILES = 2

# Flush methods
FLUSH_METHOD_NAMES = ['Scan', 'Read']
FLUSH_SCAN = 0
FLUSH_READ = 1

# Leaf constants
NOT_FULL = 0
FULL = 1

## --------- SHARED ECP/ II DEFAULTS AND OPTIONS ---------

# Sources of IOs
C_IO_SOURCES = 3
C_IO_SOURCE_NAMES = ['INSERT', 'RECLUSTER', 'FLUSH']
C_INSERT = 0
C_RECLUSTER = 1
C_FLUSH = 2

# Low: How much leader target size should be adjusted down by when resizing
LO_SIZE_PCT = 30

# Hi: How many percent larger a leader may grow beyond target
HI_SIZE_PCT = 30

## --------- ECP DEFAULTS AND OPTIONS ---------

## CLUSTER SIZE STRATEGY
# Absolute: size must be <= cluster max (1 IO enforced)
SIZE_ABSOLUTE = 0

# Average: Average size of clusters owned by a leader should be <= cluster max (1 IO)
SIZE_AVERAGE = 1

# Extra leaders to recluster. Default: No extra leaders added
RECLUSTER_PERCENT = 0

## LEADER SIZE STRATEGY
# Absolute: size of any leader must be 'LEAD_TARGET_SIZE' + HI_SIZE_PCT
LEAD_SIZE_ABSOLUTE = 0

# Average: size of leader average must be 'LEAD_TARGET_SIZE' + HI_SIZE_PCT
LEAD_SIZE_AVERAGE = 1

## LEADER SIZE POLICY
# Leader target: The target for a leader size (i.e: how many subleads a leader should govern)
LEAD_TARGET_SIZE = 100

## --------- IVFADCGP (II) DEFAULTS AND OPTIONS ---------

M_SLICES = 8

NNL_C = 5

K_ROUNDS = 256
