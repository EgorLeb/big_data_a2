#!/bin/bash

source .venv/bin/activate


# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 
export SPARK_SUBMIT_OPTS="-Xms3g -Xmx6g -XX:+UseG1GC -XX:MaxMetaspaceSize=1g -XX:ParallelGCThreads=2"


rm -rf data || true
mkdir -p data

rm -rf index/data || true
mkdir -p index/data

PARQUET_URL="
https://storage.googleapis.com/kaggle-data-sets/3521629/6146260/compressed/a.parquet.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250415%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250415T131438Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=72a25f37f317ebf7f9f6cad85b87ae2cce0b4174fe6ddcfb085e00df4631ce978c6e941477276d2dd1cccdae24478282543f624be66719da00d889298d837fb5c25e5dab6912512b5175f81abf566e8d54170b257b7c81686fc7cd78773d69d734a6a2c0a04b2b2caf92aa69d4f069d2bb36336e5c53391ec66ecbdfe44de8be9bffa93c4f38788977ed23990955fdd265dc297ea5a7f550927e1dfa010d4a6f170ce1be9194432ea7c244cbdf5047548f95589a96ea93ff9b5babdfd2c0ee87d183f3d77af52ea6c5c0eb455ce3c72ace188236f3d894cbc00362fb30a49533e3a495b9c350966ebaeb9b9d6deddc937e04d8f5c707bf329c7d79676405c56a"

PARQUET_FILE="/app/a.parquet"
PARQUET_FILE_ZIP="/app/a.parquet.zip"

if [ ! -f $PARQUET_FILE ]; then  
	echo "Dowload file a.parquet..."
	if ! wget -O $PARQUET_FILE_ZIP $PARQUET_URL; then
	  echo "Failed to download a.parquet"
	  exit 1
	fi

	echo "Extracting file a.parquet ..."
	if ! unzip -o $PARQUET_FILE_ZIP -d /app; then
	  echo "ERROR: Failed to unzip archive"
	  exit 2
	fi
fi
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
