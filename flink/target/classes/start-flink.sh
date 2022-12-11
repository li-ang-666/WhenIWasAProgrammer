#!/bin/bash

# 将 $FLINK_CONF_DIR/ 下的所有文件, 复制到自己的 flink-conf/ 里面
# 再更改自己的 flink-conf/log4j.properties
export FLINK_CONF_DIR=/root/liang/test/flink-conf/

nohup flink run \
-m yarn-cluster \
-ynm LI_ANG_FLINK_TEST \
-yqu default \
-yjm 2048 -ytm 2048 \
-ys 1 -p 1 \
-s $1 \
-yD env.java.opts="-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8" \
-yD state.backend=filesystem \
-yD state.checkpoints.dir=hdfs://emr-cluster/liang-checkpoint \
-yD state.savepoints.dir=hdfs://emr-cluster/liang-savepoint \
-c com.liang.flink.job.DataSyncToDoris /root/liang/test/daily-1.0-jar-with-dependencies.jar \
> ./start-flink.log &
