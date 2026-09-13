#!/bin/bash

folder_script="job_script"
ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
cd "$(echo $ABSOLUTE_PATH | awk -F${folder_script} '{print $1}' )"
cd /home/pmquanbackup/python-program

log_dir=/home/pmquanbackup/data/airflow/logs/daily_load_warehouse

mkdir -p $log_dir
chmod -R 777 $log_dir
if [ -z $1 ]; then
    echo "Truyền ngày tổng hợp định dạng yyyy-MM-dd"
else
    DAY=$1

    log_file=${log_dir}/log_${DAY}.txt
    spark_python_job_file=standardizer.py

    spark-submit --master yarn --deploy-mode client --queue queue-2 --packages com.clickhouse:clickhouse-jdbc:0.10.0 --num-executors 2 --executor-memory 1g --executor-cores 1 --driver-memory 1g --conf spark.sql.broadcastTimeout=300000 ${spark_python_job_file} --partition ${DAY} 2>&1 | tee -a ${log_file}

    exit_code=$(grep -q "EXIT_CODE=0" ${log_file}; echo $?)
    exit ${exit_code}
fi