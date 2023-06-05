#!/bin/bash

export FLINK_HOME=/data/liang/flink-1.14.6/
export FLINK_CONF_DIR=/data/liang/flink-1.14.6/conf/

/data/liang/flink-1.14.6/bin/flink run -t yarn-per-job -d \
  -D taskmanager.network.memory.buffer-debloat.enabled=true \
  -D env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" \
  -D state.backend="hashmap" \
  -D state.checkpoint-storage="filesystem" \
  -D state.checkpoints.dir="hdfs:///liang/flink-checkpoints/demo/" \
  -D jobmanager.memory.process.size=1024mb \
  -D taskmanager.memory.process.size=2048mb \
  -D taskmanager.memory.framework.heap.size=128mb \
  -D taskmanager.memory.framework.off-heap.size=128mb \
  -D taskmanager.memory.task.off-heap.size=0mb \
  -D taskmanager.memory.network.fraction=0.05 \
  -D taskmanager.memory.managed.size=0mb \
  -D taskmanager.memory.jvm-metaspace.size=256mb \
  -D taskmanager.memory.jvm-overhead.fraction=0.1 \
  -D taskmanager.numberOfTaskSlots=1 \
  -D parallelism.default=20 \
  -D yarn.application.name="DataConcatRepairTest" \
  -D yarn.application.queue="default" \
  -D classloader.check-leaked-classloader=false \
  -D execution.checkpointing.checkpoints-after-tasks-finish.enabled=true \
  -c com.liang.flink.job.DataConcatJob flink-1.0-jar-with-dependencies.jar config.yml \
  > log 2>&1 &