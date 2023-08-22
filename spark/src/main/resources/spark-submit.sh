#!/bin/bash

export className=SparkJob

nohup spark-submit \
  --class com.liang.spark.job.${className} \
  --name ${className} \
  --proxy-user liang \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g --driver-cores 2 \
  --conf spark.driver.memoryOverhead=512m \
  --executor-memory 6g --num-executors 10 --executor-cores 3 \
  --conf spark.executor.memoryOverhead=512m \
  --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=512m \
  --queue default \
  --conf spark.memory.fraction=0.8 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m" \
  --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m" \
  --conf spark.yarn.am.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m" \
  --conf spark.yarn.cluster.driver.extraJavaOptions="-Dlog4j.configuration=log4j-all.properties -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:MetaspaceSize=128m -XX:MaxMetaspaceSize=256m" \
  --files log4j-all.properties,config.yml \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.sql.shuffle.partitions=1200 \
  --conf spark.shuffle.io.maxRetries=10 \
  --conf spark.sql.autoBroadcastJoinThreshold=104857600 \
  ./spark-1.0-jar-with-dependencies.jar config.yml >log 2>&1 &
