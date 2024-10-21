-- beeline

!sh hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/node/company/*
!sh hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/node/human/*
!sh hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/edge/*

DROP TABLE IF EXISTS test.relation_node_company;
CREATE EXTERNAL TABLE IF NOT EXISTS test.relation_node_company(
  `company_id` string,
  `node_type` string,
  `status` string,
  `is_empty` string,
  `company_name` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'obs://hadoop-obs/flink/relation/node/company/';

DROP TABLE IF EXISTS test.relation_node_human;
CREATE EXTERNAL TABLE IF NOT EXISTS test.relation_node_human(
  `human_id` string,
  `node_type` string,
  `human_name_id` string,
  `human_name` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'obs://hadoop-obs/flink/relation/node/human/';

DROP TABLE IF EXISTS test.relation_edge;
CREATE EXTERNAL TABLE IF NOT EXISTS test.relation_edge(
  `source_id` string,
  `target_id` string,
  `relation` string,
  `other` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'obs://hadoop-obs/flink/relation/edge/';

-- spark-sql

with ids as(
  select distinct company_id id from test.relation_node_company
  union
  select distinct human_id id from test.relation_node_human
)
insert overwrite table test.relation_edge
select /*+ REPARTITION(128) */ distinct t1.*
from test.relation_edge t1
left semi join ids on t1.source_id = ids.id
left semi join ids on t1.target_id = ids.id;
