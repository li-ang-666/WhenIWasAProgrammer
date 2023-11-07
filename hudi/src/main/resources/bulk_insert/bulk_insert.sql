CREATE TABLE ods(
  id                         DECIMAL(20, 0),
  company_id                 BIGINT,
  shareholder_id             STRING,
  shareholder_entity_type    SMALLINT,
  shareholder_name_id        BIGINT,
  investment_ratio_total     DECIMAL(24, 12),
  is_controller              SMALLINT,
  is_ultimate                SMALLINT,
  is_big_shareholder         SMALLINT,
  is_controlling_shareholder SMALLINT,
  equity_holding_path        STRING,
  create_time                TIMESTAMP(3),
  update_time                TIMESTAMP(3),
  is_deleted                 SMALLINT,
  op_ts as CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://10.99.131.246:3306/company_base',
  'table-name' = 'ratio_path_company',
  'username' = 'jdhw_d_data_dml',
  'password' = '2s0^tFa4SLrp72',
  'scan.partition.column' = 'id',
  'scan.partition.num' = '40000',
  'scan.partition.lower-bound' = '1',
  'scan.partition.upper-bound' = '400000000',
  'scan.fetch-size' = '1024'
);
create table dwd(
  id                         DECIMAL(20, 0),
  company_id                 BIGINT,
  shareholder_id             STRING,
  shareholder_entity_type    SMALLINT,
  shareholder_name_id        BIGINT,
  investment_ratio_total     DECIMAL(24, 12),
  is_controller              SMALLINT,
  is_ultimate                SMALLINT,
  is_big_shareholder         SMALLINT,
  is_controlling_shareholder SMALLINT,
  equity_holding_path        STRING,
  create_time                TIMESTAMP(3),
  update_time                TIMESTAMP(3),
  is_deleted                 SMALLINT,
  op_ts                      TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_ods/ratio_path_company_tmp',
  'table.type' = 'MERGE_ON_READ',
  -- index
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.num.buckets' = '128',
  -- write
  'write.operation' = 'bulk_insert',
  'write.bulk_insert.shuffle_input' = 'false',
  'write.bulk_insert.sort_input' = 'false',
  'write.tasks' = '64',
  'write.precombine' = 'true',
  'write.precombine.field' = 'op_ts',
  -- compaction
  'compaction.schedule.enabled' = 'false',
  'compaction.async.enabled' = 'false',
  -- clean
  'clean.async.enabled' = 'false',
  -- hive
  'hive_sync.enabled' = 'true',
  'hive_sync.db' = 'hudi_ods',
  'hive_sync.table' = 'ratio_path_company_tmp',
  'hive_sync.metastore.uris' = 'thrift://10.99.202.153:9083',
  'hive_sync.mode' = 'hms',
  'hive_sync.skip_ro_suffix' = 'true'
);
insert into dwd
select
id,company_id,shareholder_id,shareholder_entity_type,shareholder_name_id,investment_ratio_total,is_controller,is_ultimate,is_big_shareholder,is_controlling_shareholder,equity_holding_path,
CAST(CONVERT_TZ(CAST(create_time AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) create_time,
CAST(CONVERT_TZ(CAST(update_time AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) update_time,
is_deleted,
CAST(CONVERT_TZ(CAST(op_ts AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) op_ts
from ods;