nohup spark-submit \
--class com.liang.spark.Launcher \
--name "liang-spark" \
--master yarn \
--deploy-mode cluster \
--driver-memory 3500m \
--conf spark.driver.memoryOverhead=500 \
--executor-memory 3000m --num-executors 7 --executor-cores 3 \
--conf spark.executor.memoryOverhead=1000 \
--queue default \
--conf spark.memory.fraction=0.7 \
--conf spark.memory.storageFraction=0.4 \
--files ./log4j.properties \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j.properties" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j.properties" \
--conf spark.yarn.maxAppAttempts=1 \
./spark-1.0-jar-with-dependencies.jar > log &
