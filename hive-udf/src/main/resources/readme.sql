-- 上传
!sh hdfs dfs -put -f /home/hive/liang/hive-udf-1.0.jar /hive/jars/liang-hive-udf-1.0.jar

-- 加载
add jar hdfs:///liang/hive-udf-1.0.jar;

-- 创建UDAF函数
drop function if exists doris.to_bitmap;
create function doris.to_bitmap as 'com.liang.udf.ToBitmapUDAF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
drop function if exists doris.bitmap_union;
create function doris.bitmap_union as 'com.liang.udf.BitmapUnionUDAF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';

-- 创建UDF函数
drop function if exists doris.bitmap_count;
create function doris.bitmap_count as 'com.liang.udf.BitmapCountUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
drop function if exists doris.bitmap_and;
create function doris.bitmap_and as 'com.liang.udf.BitmapAndUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
drop function if exists doris.bitmap_or;
create function doris.bitmap_or as 'com.liang.udf.BitmapOrUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';
drop function if exists doris.bitmap_xor;
create function doris.bitmap_xor as 'com.liang.udf.BitmapXorUDF' USING JAR 'hdfs:///hive/jars/liang-hive-udf-1.0.jar';

-- 测试
show functions;
desc function extended doris.to_bitmap;

------------------------------------------------------------------------------------------------------------------------

!sh hdfs dfs -rm /liang/hive-udf-1.0.jar
!sh hdfs dfs -put /home/hive/liang/hive-udf-1.0.jar /liang/
add jar hdfs:///liang/hive-udf-1.0.jar;
drop temporary function fff;
create temporary function fff as 'com.liang.udf.DemoUDF' using jar 'hdfs:///liang/hive-udf-1.0.jar';
desc function  extended fff;
