#!/bin/bash

nohup spark-submit \
--class com.liang.spark.job.DataConcatJob \
--name DataConcatTest \
--proxy-user liang \
--master yarn \
--deploy-mode cluster \
--driver-memory 2g --driver-cores 2 \
--conf spark.driver.memoryOverhead=512m \
--executor-memory 5g --num-executors 10 --executor-cores 3 \
--conf spark.executor.memoryOverhead=512m \
--conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=1g \
--queue default \
--conf spark.memory.fraction=0.7 \
--conf spark.memory.storageFraction=0.2 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
--conf spark.yarn.am.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
--conf spark.yarn.cluster.driver.extraJavaOptions="-Dlog4j.configuration=log4j-ud.properties" \
--files /home/hive/liang/data-concat-test/log4j-ud.properties,/home/hive/liang/data-concat/config.yml \
--conf spark.yarn.maxAppAttempts=1 \
./spark-1.0-jar-with-dependencies.jar "config.yml" > log 2>&1 &