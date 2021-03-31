bench_file_path="/home/lzy/rocksdb/db_bench"

bench_db_path="/mnt/optane-ssd/db"
# bench_db_path="/mnt/ext4-128/db"
# wal_dir="/mnt/optane-ssd/db"
wal_dir="/mnt/ext4-128/db"
threads=1
value_size=100
bench_benchmarks="fillseq"
bench_num=1000000
bench_readnum=1000000
bench_compression="None"
max_background_jobs=16
max_bytes_for_level_base="`expr 8 \* 1024 \* 1024 \* 1024`" #default 256 \* 1024 \* 1024
mmap_write=true
batch=1
sync=false
disable_wal=false


RUN_ONE_TEST() {
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
    --use_existing_db=0 \
    --mmap_write=$mmap_write \
    --batch_size=$batch \
    --sync=$sync \
    --disable_wal=$disable_wal 
    "
        
    
    # cmd="$bench_file_path $const_params | tee out.out 2>&1"
    # echo $cmd >out.out
    cmd="$bench_file_path $const_params"
    echo $cmd
    eval $cmd
}

CLEAN_CACHE() {
    if [ -n "$bench_db_path" ];then
        rm -f $bench_db_path/*
    fi
    sleep 1
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 1
}

CLEAN_CACHE
RUN_ONE_TEST