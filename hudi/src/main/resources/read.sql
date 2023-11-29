create table ods(
  id DECIMAL(20, 0),
  company_id BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_ods/ratio_path_company',
  'table.type' = 'MERGE_ON_READ',
  -- cdc
  'changelog.enabled' = 'true',
  -- read
  'read.tasks' = '8',
  'read.streaming.enabled' = 'true',
  --'read.start-commit' = '20231129170000',
  'read.streaming.check-interval' = '5'
);