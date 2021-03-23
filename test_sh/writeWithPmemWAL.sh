bench_db_path="/mnt/optane-ssd/db"
wal_dir="/mnt/ext4-128/db"
threads=1
value_size=100
bench_benchmarks="fillseq"
bench_num=10000000
bench_readnum=1000000
bench_compression="None"
max_background_jobs=16
max_bytes_for_level_base="`expr 8 \* 1024 \* 1024 \* 1024`" #default 256 \* 1024 \* 1024
report_write_latency="false"


RUN_ONE_TEST() {
    const_params="
    --db=$bench_db_path \
    --wal_dir=$wal_dir \
    --threads=$threads \
    --value_size=$value_size \
    --benchmarks=$bench_benchmarks \
    --num=$bench_num \
    --reads=$bench_readnum \
    --compression_type=$bench_compression \
    --max_background_jobs=$max_background_jobs \
    --report_write_latency=$report_write_latency \
    --use_existing_db=0
    "
        # --max_bytes_for_level_base=$max_bytes_for_level_base \
    cmd="$bench_file_path $const_params >>out.out 2>&1"
    echo $cmd >out.out
    echo $cmd
    eval $cmd
}

CLEAN_CACHE() {
    if [ -n "$bench_db_path" ];then
        rm -f $bench_db_path/*
    fi
    sleep 2
    sync
    echo 3 > /proc/sys/vm/drop_caches
    sleep 2
}

CLEAN_CACHE
RUN_ONE_TEST