CREATE TABLE ods (
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
  op_ts                      TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'e1d4c.json.prism_shareholder_path.ratio_path_company',
  'properties.bootstrap.servers' = '10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092',
  'properties.group.id' = 'hudi-demo-job',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'canal-json',
  'canal-json.ignore-parse-errors' = 'true',
  'canal-json.encode.decimal-as-plain-number' = 'true'
);
CREATE TABLE dwd(
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
  'path' = 'obs://hadoop-obs/hudi_ods/ratio_path_company',
  'table.type' = 'MERGE_ON_READ',
  -- index
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.num.buckets' = '128',
  -- write
  'write.tasks' = '4',
  'write.task.max.size' = '512',
  'write.batch.size' = '8',
  'write.log_block.size' = '64',
  'write.precombine' = 'true',
  'write.precombine.field' = 'op_ts',
  -- compaction
  'compaction.async.enabled' = 'false',
  'compaction.delta_commits' = '10',
  -- clean
  'clean.retain_commits' = '0',
  -- hive
  'hive_sync.enabled' = 'true',
  'hive_sync.db' = 'hudi_ods',
  'hive_sync.table' = 'ratio_path_company',
  'hive_sync.metastore.uris' = 'thrift://10.99.202.153:9083',
  'hive_sync.mode' = 'hms'
);
insert into dwd
select
id,company_id,shareholder_id,shareholder_entity_type,shareholder_name_id,investment_ratio_total,is_controller,is_ultimate,is_big_shareholder,is_controlling_shareholder,equity_holding_path,
CAST(CONVERT_TZ(CAST(create_time AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) create_time,
CAST(CONVERT_TZ(CAST(update_time AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) update_time,
is_deleted,
CAST(CONVERT_TZ(CAST(op_ts AS STRING), 'Asia/Shanghai', 'UTC') AS TIMESTAMP(3)) op_ts
from ods;