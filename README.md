# Benchmarking Optane SSD vs Optane DCPMM

Here hosts resources used in our recent CIDR 2022 [paper](http://www.sfu.ca/~kha85/modern-storage-index.pdf) below for comparing SSD and PM indexes, including a benchmark driver adapted from [1], a B-tree implementation based on [2], an on-disk hash table [3], FPTree [4], BzTree [5] and Dash [6].

```
SSDs Striking Back: The Storage Jungle and Its Implications on Persistent Indexes.
Kaisong Huang, Darien Imai, Tianzheng Wang and Dong Xie.
12th Annual Conference on Innovative Data Systems Research (CIDR ’22). January 9-12, 2022, Chaminade, USA.
```

[1] https://github.com/basicthinker/YCSB-C<br/>
[2] https://github.com/jeffreyorihuela/b-tree-on-disk<br/>
[3] https://github.com/dongx-psu/hashtable<br/>
[4] https://github.com/sfu-dis/fptree<br/>
[5] https://github.com/sfu-dis/bztree<br/>
[6] https://github.com/baotonglu/dash

## Required Flags
```
-path <pathname>: Path to the containing folder of btree or hashtable, or to the pool file of PM-based indexes.
-tree <indexname>: Choose from [btree hashtable bztree dash pibench].
-load <bool>: truncate files, and load new indexes, default is false.
-run <bool>: use the preloaded files to run benchmarks, default is false.
```

## Useful Options
* `-stride <n>`:The stride when setting CPU affinity, default is 2.
* `-starting_cpu <n>`: The starting CPU # for affinity, default is 0.
* `-benchmarkseconds <n>`: Duration of test, default is 20, can also be configured in the spec files.
* `-buffer_page <n>`: The number of pages for the buffer pool.


## Compile and Run:
**Make sure the git submodule abseil-cpp is initialized.**
```
git submodule update --init
```

### Run Btree tests
```
$ mkdir build; cd build;
$ cmake .. -DCMAKE_BUILD_TYPE=<Debug|Release>
$ make
$ cd Ycsb
```
Load # records. (# = `recordcount` in the `*.spec` file)
```
$ ./ycsb -path </path/to/tree/dir> -tree btree -threads <#threads> -p </path/to/workload/spec> -load true -buffer_page <#page>
```
Run benchmarks.
```
$ ./ycsb -path </path/to/tree/dir> -tree btree -threads <#threads> -p </path/to/workload/spec> -run true -buffer_page <#page>
```
NOTE: spec file examples can be found in /Ycsb/workloads.

### Run hash table tests
```
$ mkdir build; cd build;
$ CXX=clang++ CC=clang cmake .. -DCMAKE_BUILD_TYPE=<Debug|Release>
$ make
$ cd Ycsb
```
Load # records. (# = `recordcount` in the `*.spec` file)
```
$ ./ycsb -path </path/to/hashtable/dir> -tree hashtable -threads <#threads> -p </path/to/workload/spec> -load true -buffer_page <#page>
```
Run benchmarks.
```
$ ./ycsb -path </path/to/hashtable/dir> -tree hashtable -threads <#threads> -p </path/to/workload/spec> -run true -buffer_page <#page>
```

### Run BzTree tests
Load the tree.
```
$ ./ycsb -p <spec> -tree bztree -poolsize $POOL_SIZE_IN_BYTES -path $POOLFILE -threads 1 -starting_cpu -load true
```
Run benchmarks.
```
$./ycsb -benchmarkseconds 60 -p <spec> -tree bztree -path $POOLFILE -threads $THREADS -starting_cpu $STARTING_CPU -stride 2 -run true
```

### Run Dash tests
Load and run in one command.
```
$./ycsb -benchmarkseconds 60 -p <spec> -tree dash -poolsize $POOL_SIZE_IN_BYTES -path $POOLFILE -threads $THREADS -starting_cpu $STARTING_CPU -stride 2 -epoch 1024 -load true -run true
```

### Run tests with PiBench Wrapper<br/>
Our benchmark tool is compatible with PiBench wrappers.<br/>
Load the tree.
```
$./ycsb -benchmarkseconds 60 -p <spec> -tree pibench -poolsize $POOL_SIZE_IN_BYTES -wrapper $FPWRAPPER -path $POOLFILE -threads $THREADS -starting_cpu $STARTING_CPU -stride 2 -load true
```
Run benchmarks.
```
$./ycsb -benchmarkseconds 60 -p <spec> -tree pibench -wrapper $TREEWRAPPER -path $POOLFILE -threads $THREADS -starting_cpu $STARTING_CPU -stride 2 -run true
```

## Troubleshooting
1. If you ever hit any issue that comes from Dash during compiling, use GCC instead of clang, and add '-march=native -mtune=native' to the compiler flags.
2. If you see similar errors like this one: `No rule to make target '/usr/lib/x86_64-linux-gnu/libpmemobj.so', needed by 'Ycsb/ycsb'`, please go to `Bztree/CMakeLists.txt` and change line 19 and line 69 to your own PMDK lib path.
