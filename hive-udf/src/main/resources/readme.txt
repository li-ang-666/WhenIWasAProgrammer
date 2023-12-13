-- 上传
hdfs dfs -rm /liang/hive-udf-1.0.jar
hdfs dfs -put ./hive-udf-1.0.jar /liang/

-- 加载
add jar hdfs:///liang/hive-udf-1.0.jar;

-- 创建UDAF函数
create temporary function to_bitmap as 'com.liang.udf.ToBitmapUDAF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
create temporary function bitmap_union as 'com.liang.BitmapUnionUDAF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';

-- 创建UDF函数
create temporary function bitmap_count as 'com.liang.BitmapCountUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
create temporary function bitmap_and as 'com.liang.BitmapAndUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
create temporary function bitmap_or as 'com.liang.BitmapOrUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
create temporary function bitmap_xor as 'com.liang.BitmapXorUDF' USING JAR 'hdfs:///liang/hive-udf-1.0.jar';
