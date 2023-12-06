#!/bin/bash
export FLINK_HOME=/home/hive/flink/flink-1.17.1
export FLINK_CONF_DIR=/home/hive/flink/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`
/home/hive/flink/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=1536m \
  -D taskmanager.memory.process.size=2560m \
  -D taskmanager.numberOfTaskSlots=2 \
  -D parallelism.default=1 \
  -D taskmanager.memory.network.min=16m \
  -D taskmanager.memory.network.max=16m \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/upsert/${2} \
  -D yarn.application.name=hudi_upsert_${2} \
  -c com.liang.hudi.job.HudiJob hudi-1.0.jar UPSERT ${1} ${2} "${3}"