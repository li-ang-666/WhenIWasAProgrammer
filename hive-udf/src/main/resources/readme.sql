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

-- 测试
show functions;
desc function extended to_bitmap;

-- 测试
drop table if exists test.bitmap_test;
create table if not exists test.bitmap_test(id int, tb string, bitmap binary);
with t as(
  select 1 id, 'company_bond_plates' tb, to_bitmap(id) bitmap from hudi_ods.company_bond_plates
  union all
  select 2 id, 'senior_executive' tb, to_bitmap(id) bitmap from hudi_ods.senior_executive
  union all
  select 3 id, 'senior_executive_hk' tb, to_bitmap(id) bitmap from hudi_ods.senior_executive_hk
)insert overwrite table test.bitmap_test select * from t;
select id, tb, bitmap_count(bitmap) from test.bitmap_test;
