create table enterprise(
  id                         DECIMAL(20, 0),
  op_ts                      TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_ods/ab_cd_ef002',
  'table.type' = 'MERGE_ON_READ',
  -- cdc
  'changelog.enabled' = 'true',
  -- read
  'read.tasks' = '1',
  'read.streaming.enabled' = 'true',
  'read.start-commit' = 'earliest',
  'read.streaming.check-interval' = '5'
);