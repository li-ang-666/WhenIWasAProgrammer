#!/bin/bash
yarn app -list | grep hudi_compaction_${1}
export p=`yarn app -list | grep hudi_compaction_${1}`
if [[ ${p} == application_* ]] ; then
  echo 'exists, return' && exit 0
else
  echo 'not exists, continue'
fi

export FLINK_HOME=/home/hive/flink/flink-1.17.1
export FLINK_CONF_DIR=/home/hive/flink/flink-conf
export HADOOP_CLASSPATH=`hadoop classpath`
/home/hive/flink/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=2g \
  -D taskmanager.memory.process.size=6g \
  -D taskmanager.numberOfTaskSlots=8 \
  -D parallelism.default=1 \
  -D taskmanager.memory.network.max=64m \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/compaction/${1} \
  -D yarn.application.name=hudi_compaction_${1}_hi \
  -c org.apache.hudi.sink.compact.HoodieFlinkCompactor hudi-1.0.jar --path obs://hadoop-obs/hudi_ods/${1} \
  --compaction-tasks 32 --compaction-max-memory 512 \
  --clean-retain-commits 10 --archive-min-commits 20 --archive-max-commits 30