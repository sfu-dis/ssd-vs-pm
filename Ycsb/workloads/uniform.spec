# Yahoo! Cloud System Benchmark
# Workload A: Update heavy workload
#   Application example: Session store recording recent actions
#                        
#   Read/update ratio: 0/100
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: zipfian
keylength=8
fieldcount=1
fieldlength=8

insertstart=0 # Change it accordingly to avoid inserting existing keys.

recordcount=1000

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=1
insertproportion=0
updateproportion=0
scanproportion=0

requestdistribution=uniform 

benchmarkseconds=60
