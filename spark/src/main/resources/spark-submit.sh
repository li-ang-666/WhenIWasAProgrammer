#!/bin/bash

export SPARK_CONF_DIR=/home/hive/liang/spark-conf/

nohup spark-submit \
  --class com.liang.spark.job.DataConcat \
  --name DataConcatTest \
  --proxy-user liang \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g --driver-cores 2 \
  --conf spark.driver.memoryOverhead=512m \
  --executor-memory 4g --num-executors 10 --executor-cores 2 \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1g \
  --queue default \
  --conf spark.memory.fraction=0.7 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
  --conf spark.yarn.am.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
  --conf spark.yarn.cluster.driver.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
  --files /home/hive/liang/data-concat-test/log4j-ud.properties \
  --conf spark.yarn.maxAppAttempts=1 \
  ./spark-1.0-jar-with-dependencies.jar >log &
