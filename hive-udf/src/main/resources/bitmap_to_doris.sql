readme.sql-- 上传
!sh hdfs dfs -put -f /home/hive/liang/hive-udf-1.0.jar /hive/jars/liang-hive-udf-1.0.jar
-- 加载
add jar hdfs:///hive/jars/liang-hive-udf-1.0.jar;
-- 创建UDF函数
drop function if exists doris.bitmap_to_doris;
create function doris.bitmap_to_doris as 'com.liang.udf.BitmapToDorisUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- 测试
desc function extended doris.bitmap_to_doris;
set spark.executor.memory=8g;
