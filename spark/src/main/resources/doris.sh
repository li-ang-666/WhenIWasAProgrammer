#!/bin/bash

sql="${2}${4}"
insert_sql=$(echo "$sql" | grep -oE 'insert\s+into\s+\w+\.\w+')
res=$(echo "$insert_sql" | grep -oE '\w+\.\w+')

nohup spark-submit \
  --class com.liang.spark.job.DorisJob \
  --name doris.${res} \
  --proxy-user hive \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g --driver-cores 1 \
  --conf spark.driver.memoryOverhead=512m \
  --executor-memory 5g --num-executors 4 --executor-cores 8 \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=512m \
  --queue offline \
  --conf spark.memory.fraction=0.8 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.yarn.am.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --conf spark.yarn.cluster.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps" \
  --files log4j-all.properties,config.yml \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.yarn.priority=999 \
  --conf spark.sql.shuffle.partitions=256 \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \
  ./spark-1.0-jar-with-dependencies.jar "${1}" "${2}" "${3}" "${4}" >log 2>&1 &