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

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/company/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_node_company;

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/human/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_node_human;

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/LEGAL/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'LEGAL';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/HIS_LEGAL/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'HIS_LEGAL';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/INVEST/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'INVEST';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/HIS_INVEST/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'HIS_INVEST';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/BRANCH/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'BRANCH';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/AC/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'AC';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/SERVE/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'SERVE';

INSERT OVERWRITE DIRECTORY 'obs://hadoop-obs/flink/relation/result/HIS_SERVE/'
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  LINES TERMINATED BY '\n'
SELECT /*+ REPARTITION(128) */ distinct * FROM test.relation_edge WHERE relation = 'HIS_SERVE';

