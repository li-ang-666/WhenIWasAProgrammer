#!/bin/bash

export FLINK_HOME=/data/liang/flink-1.17.1
export FLINK_CONF_DIR=/data/liang/flink-conf

export jobName=FlinkJob
export configName=config.yml

export folderName=$(echo ${jobName} | sed -E 's/([A-Z])/-\1/g' | sed -E 's/^-//g' | tr 'A-Z' 'a-z')
/data/liang/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=1024mb \
  -D taskmanager.memory.process.size=2048mb \
  -D taskmanager.numberOfTaskSlots=1 \
  -D yarn.application.name=${jobName}Test \
  -D state.checkpoints.dir=hdfs:///liang/flink-checkpoints/${folderName}-test \
  -D yarn.ship-files=${configName} \
  -c com.liang.flink.job.${jobName} flink-1.0-jar-with-dependencies.jar ${configName}
