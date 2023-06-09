#!/bin/bash

export FLINK_HOME=/data/liang/flink-1.17.1/
export FLINK_CONF_DIR=/data/liang/flink-conf/

/data/liang/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=1024mb \
  -D taskmanager.memory.process.size=2048mb \
  -D taskmanager.numberOfTaskSlots=1 \
  -D parallelism.default=1 \
  -D yarn.application.queue=default \
  -D yarn.application.name=${JobName} \
  -D state.checkpoints.dir="hdfs:///liang/flink-checkpoints/${job-name}/" \
  -D yarn.ship-files="config.yml" \
  -c com.liang.flink.job.DataConcatJob flink-1.0-jar-with-dependencies.jar config.yml
