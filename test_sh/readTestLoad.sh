# export PMEM_NO_CLWB=1
# export PMEM_NO_CLFLUSHOPT=1
# export PMEM_NO_MOVNT=1 
# export PMEM_MOVNT_THRESHOLD=0
# export PMEM_NO_FLUSH=1
ulimit -n 65535

bench_file_path="/home/lzy/rocksdb/db_bench"

bench_db_path="/mnt/optane-ssd/db"
# bench_db_path="/mnt/ext4-128/db"
wal_dir="/mnt/optane-ssd/db"
# wal_dir="/mnt/ext4-128/db"
threads=1
value_size=1000
bench_benchmarks="filluniquerandom,stats,levelstats"
bench_num=10000000
bench_readnum=100000
bench_compression="None"
max_background_jobs=17
max_background_compactions=`expr $max_background_jobs - 1`
# max_bytes_for_level_base="`expr 8 \* 1024 \* 1024 \* 1024`" #default 256 \* 1024 \* 1024
mmap_write=false
mmap_read=false
batch=1
sync=false
disable_wal=true
enable_pm_wal=true


RUN_ONE_TEST(){
    const_params="
    --db=$bench_db_path \
    --wal_dir=$wal_dir \
    --threads=$threads \
    --compression_type=$bench_compression \
    --value_size=$value_size \
    --benchmarks=$bench_benchmarks \
    --num=$bench_num \
    --reads=$bench_readnum \
    --max_background_jobs=$max_background_jobs \
    --max_background_compactions=$max_background_compactions \
    --use_existing_db=0 \
    --mmap_write=$mmap_write \
    --mmap_read=$mmap_read \
    --batch_size=$batch \
    --sync=$sync \
    --disable_wal=$disable_wal \
    --enable_pm_wal=$enable_pm_wal \
    --use_direct_io_for_flush_and_compaction=true \
    #--writable_file_max_buffer_size=65536
    "
    cmd="$bench_file_path $const_params"
    echo $cmd
    eval $cmd
}

REMOVE_FILE(){
    if [ -n "$bench_db_path" ];then
        rm -f $bench_db_path/*
    fi
    if [ -n "$wal_dir" ];then
        rm -f $wal_dir/*
    fi
}

CLEAN_CACHE() {
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 1
}

REMOVE_FILE
CLEAN_CACHE
RUN_ONE_TEST
# CLEAN_CACHE
# RUN_READ_TO_VERIFY
