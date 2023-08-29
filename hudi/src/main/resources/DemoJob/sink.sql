create table hudi_table(
  id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) with (
  'connector' = 'hudi',
  'path' = 'hdfs:///liang/hudi_table/',
  'table.type' = 'MERGE_ON_READ'
)