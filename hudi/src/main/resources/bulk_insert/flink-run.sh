#!/bin/bash
####################################
#                                  #
#             variable             #
#                                  #
####################################
# 类名
export className=DemoJob
# 配置文件名 config.yml or repair.yml
export configName=hudi.yml

####################################
#                                  #
#              ignore              #
#                                  #
####################################
# flink env
export FLINK_HOME=/home/hive/hudi/flink-1.17.1
export FLINK_CONF_DIR=/home/hive/hudi/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`
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
/home/hive/hudi/flink-1.17.1/bin/flink run-application -t yarn-application ${restoreDir} \
  -D jobmanager.memory.process.size=1g \
  -D taskmanager.memory.process.size=4g \
  -D taskmanager.numberOfTaskSlots=8 \
  -D parallelism.default=16 \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/${folderName} \
  -D yarn.ship-files=${configName} \
  -D yarn.application.name=HudiTest \
  -c com.liang.hudi.job.${className} hudi-1.0.jar ${configName}