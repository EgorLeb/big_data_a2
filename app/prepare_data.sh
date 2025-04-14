#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
export SPARK_SUBMIT_OPTS="-Xms3g -Xmx6g -XX:+UseG1GC -XX:MaxMetaspaceSize=1g -XX:ParallelGCThreads=2"


rm -rf data || true
mkdir -p data

rm -rf index/data || true
mkdir -p index/data


unset PYSPARK_PYTHON

# DOWNLOAD a.parquet or any parquet file before you run this
echo "[1/3] Uploading a.parquet..." && \
hdfs dfs -put -f a.parquet / && \

echo "Creating HDFS directories..." && \
hdfs dfs -mkdir -p /data && \
hdfs dfs -mkdir -p /index/data && \

echo "[2/3] Running Spark job..." && \
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 3g \
    --num-executors 1 \
    --conf spark.sql.autoBroadcastJoinThreshold=-1 \
    --conf spark.sql.shuffle.partitions=50 \
    --conf spark.memory.fraction=0.75 \
    --conf spark.executor.extraJavaOptions="-Xloggc:/tmp/spark-gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
    --conf spark.driver.extraJavaOptions="-Xloggc:/tmp/spark-driver-gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
    prepare_data.py  && \
echo "[3/3] finalizing..." && \
echo "Putting data to hdfs"  && \
hdfs dfs -put -f data /data && \
hdfs dfs -ls -R /data && \
hdfs dfs -ls -R /index/data && \
echo "Done data preparation!"
