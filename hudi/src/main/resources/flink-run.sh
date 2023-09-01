#!/bin/bash
####################################
#                                  #
#             variable             #
#                                  #
####################################
# 类名
export className=FlinkJob
# 配置文件名 config.yml or repair.yml
export configName=config.yml

####################################
#                                  #
#              ignore              #
#                                  #
####################################
# flink env
export FLINK_HOME=/data/hudi/flink-1.16.2
export FLINK_CONF_DIR=/data/hudi/flink-conf
# checkpoint dir
export folderName=$(echo ${className} | sed -E 's/([A-Z])/-\1/g' | sed -E 's/^-//g' | tr 'A-Z' 'a-z')
# yarn application name
export jobName=$(if [[ ${configName} == config* ]] ; then echo ${className} ; else echo ${className}RepairTest ; fi)
# restore dir
export restoreDir=$(if [[ $1 == hdfs* ]] ; then echo '-s '$1 ; else echo '' ; fi)

####################################
#                                  #
#    maybe change Memory or Slot   #
#                                  #
####################################
/data/hudi/flink-1.16.2/bin/flink run-application -t yarn-application ${restoreDir} \
  -D jobmanager.memory.process.size=2048mb \
  -D taskmanager.memory.process.size=4096mb \
  -D taskmanager.numberOfTaskSlots=4 \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/${folderName} \
  -D yarn.ship-files=${configName} \
  -D yarn.application.name=${jobName} \
  -c com.hudi.flink.job.${className} hudi-1.0.jar ${configName}
