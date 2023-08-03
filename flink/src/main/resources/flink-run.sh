#!/bin/bash

export FLINK_HOME=/data/liang/flink-1.17.1
export FLINK_CONF_DIR=/data/liang/flink-conf

# 类名
export className=FlinkJob
# 配置文件名 config.yml or repair.yml
export configName=config.yml

# 以下除了 memory 和 slot, 其余不用改
export folderName=$(echo ${className} | sed -E 's/([A-Z])/-\1/g' | sed -E 's/^-//g' | tr 'A-Z' 'a-z')
export jobName=$(
if [[ ${configName} == config* ]];
then
  echo ${className}
else
  echo ${className}RepairTest
fi)
/data/liang/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=2048mb \
  -D taskmanager.memory.process.size=4096mb \
  -D taskmanager.numberOfTaskSlots=6 \
  -D state.checkpoints.dir=hdfs:///liang/flink-checkpoints/${folderName} \
  -D yarn.ship-files=${configName} \
  -D yarn.application.name=${jobName} \
  -c com.liang.flink.job.${className} flink-1.0-jar-with-dependencies.jar ${configName}
