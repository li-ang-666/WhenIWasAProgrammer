#!/bin/bash
export FLINK_HOME=/data/omm/flink-1.17.1
export FLINK_CONF_DIR=/data/omm/flink-conf
/data/omm/flink-1.17.1/bin/flink run-application -t yarn-application \
  -D jobmanager.memory.process.size=2g \
  -D taskmanager.memory.process.size=4g \
  -D taskmanager.numberOfTaskSlots=4 \
  -D parallelism.default=1 \
  -D taskmanager.memory.network.max=64m \
  -D state.checkpoints.dir=hdfs:///hudi/flink-checkpoints/upsert/${2} \
  -D yarn.application.name=hudi_upsert_${2} \
  -c com.liang.hudi.job.HudiJob hudi-1.0.jar UPSERT ${1} ${2} "${3}"