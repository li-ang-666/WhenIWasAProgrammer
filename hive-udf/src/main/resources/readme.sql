-- 上传
hdfs dfs -rm /liang/hive-udf-1.0.jar
hdfs dfs -put ./hive-udf-1.0.jar /liang/

-- 加载
add jar hdfs:///liang/hive-udf-1.0.jar;

-- 创建UDAF函数
drop temporary function if exists to_bitmap;
create temporary function to_bitmap as 'com.liang.udf.ToBitmapUDAF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
drop temporary function if exists bitmap_union;
create temporary function bitmap_union as 'com.liang.udf.BitmapUnionUDAF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';

-- 创建UDF函数
drop temporary function if exists bitmap_count;
create temporary function bitmap_count as 'com.liang.udf.BitmapCountUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
drop temporary function if exists bitmap_and;
create temporary function bitmap_and as 'com.liang.udf.BitmapAndUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
drop temporary function if exists bitmap_or;
create temporary function bitmap_or as 'com.liang.udf.BitmapOrUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
drop temporary function if exists bitmap_xor;
create temporary function bitmap_xor as 'com.liang.udf.BitmapXorUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
