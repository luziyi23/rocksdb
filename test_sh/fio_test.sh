blocksize="4k"

fio -directory=/mnt/ext4-128 -direct=1 -iodepth 1 -thread -rw=write -ioengine=libpmem -bs=$blocksize -size=2G -numjobs=1 -runtime=20 -group_reporting -name=mytest