#!/bin/bash
export FLINK_HOME=/home/hive/flink/flink-1.17.1
export FLINK_CONF_DIR=/home/hive/flink/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`
/home/hive/flink/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=2g \
  -D taskmanager.memory.process.size=6g \
  -D taskmanager.numberOfTaskSlots=8 \
  -D parallelism.default=16 \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/bulk-insert/${2} \
  -D yarn.application.name=hudi_bulk_insert_${2} \
  -c com.liang.hudi.job.HudiJob hudi-1.0.jar BULK_INSERT ${1} ${2} "${3}"