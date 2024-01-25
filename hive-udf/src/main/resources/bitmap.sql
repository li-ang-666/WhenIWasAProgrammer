-- 上传
!sh hdfs dfs -put -f /home/hive/liang/hive-udf-1.0.jar /hive/jars/liang-hive-udf-1.0.jar
-- 加载
add jar hdfs:///hive/jars/liang-hive-udf-1.0.jar;
-- 创建UDAF函数
-- to_bitmap
drop function if exists doris.to_bitmap;
create function doris.to_bitmap as 'com.liang.udf.ToBitmapUDAF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- bitmap_union
drop function if exists doris.bitmap_union;
create function doris.bitmap_union as 'com.liang.udf.BitmapUnionUDAF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- 创建UDF函数
-- bitmap_count
drop function if exists doris.bitmap_count;
create function doris.bitmap_count as 'com.liang.udf.BitmapCountUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- bitmap_and
drop function if exists doris.bitmap_and;
create function doris.bitmap_and as 'com.liang.udf.BitmapAndUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- bitmap_or
drop function if exists doris.bitmap_or;
create function doris.bitmap_or as 'com.liang.udf.BitmapOrUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- bitmap_xor
drop function if exists doris.bitmap_xor;
create function doris.bitmap_xor as 'com.liang.udf.BitmapXorUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- bitmap_to_doris
drop function if exists doris.bitmap_to_doris;
create function doris.bitmap_to_doris as 'com.liang.udf.BitmapToDorisUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
-- 测试
desc function extended doris.to_bitmap;
desc function extended doris.bitmap_to_doris;
set spark.executor.memory=8g;
