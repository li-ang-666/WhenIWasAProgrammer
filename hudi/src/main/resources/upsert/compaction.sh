#!/bin/bash

export FLINK_HOME=/home/hive/flink/flink-1.17.1
export FLINK_CONF_DIR=/home/hive/flink/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`

/home/hive/flink/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=1g \
  -D taskmanager.memory.process.size=8g \
  -D taskmanager.numberOfTaskSlots=8 \
  -D parallelism.default=1 \
  -D state.checkpoints.dir=hdfs:///liang/flink-checkpoints/hudi \
  -D yarn.application.priority=5 \
  -D yarn.application.name=HudiCompactionTest \
  -c org.apache.hudi.sink.compact.HoodieFlinkCompactor hudi-1.0.jar --path obs://hadoop-obs/hudi_ods/ratio_path_company \
  --compaction-tasks 32 --compaction-max-memory 1024 --service --min-compaction-interval-seconds 10