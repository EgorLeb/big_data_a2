#!/bin/bash
echo "This script include commands to run mapreduce jobs using hadoop streaming to index documents"

INPUT_PATH=${1:-/index/data}
TMP_PATH=/tmp/index

hdfs dfs -ls /

hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2


hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -archives ".venv.tar.gz#venv" \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -input $INPUT_PATH \
    -output $TMP_PATH/output1 \
    -mapper "venv/bin/python3 mapper1.py" \
    -reducer "venv/bin/python3 reducer1.py"


hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -archives ".venv.tar.gz#venv" \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -input $INPUT_PATH \
    -output $TMP_PATH/output2 \
    -mapper "venv/bin/python3 mapper2.py" \
    -reducer "venv/bin/python3 reducer2.py"


hdfs dfs -rm -r $TMP_PATH