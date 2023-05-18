#!/bin/bash

flink run -t yarn-per-job -d \
 -D execution.checkpointing.unaligned=true \
 -D execution.checkpointing.unaligned.forced=true \
 -D taskmanager.network.memory.buffer-debloat.enabled=true \
 -D state.backend="rocksdb" \
 -D state.backend.incremental=true \
 -D state.backend.rocksdb.block.blocksize=32768 \
 -D env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" \
 -D jobmanager.memory.process.size=2000mb \
 -D taskmanager.memory.process.size=4000mb \
 -D yarn.application.name=WhenIWas \
 -D yarn.application.queue=default \
 -D taskmanager.memory.managed.fraction=0.1 \
 -D taskmanager.numberOfTaskSlots=2 \
 -p 2 \
 -c package com.liang.flink.job.FlinkStream ./flink-*.jar