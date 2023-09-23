create table hudi_table(
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
  create_time                TIMESTAMP(0),
  update_time                TIMESTAMP(0),
  is_deleted                 SMALLINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_tables/uuid',
  'table.type' = 'MERGE_ON_READ',
  -- index
  'index.type' = 'bucket',
  'hoodie.bucket.index.num.buckets' = '32',
  -- unused memory
  'compaction.max_memory' = '0',
  'write.merge.max_memory' = '0',
  -- write
  'write.tasks' = '32',
  'write.task.max.size' = '132',
  'write.batch.size' = '32',
  'write.log_block.size' = '128',
  'write.precombine' = 'true',
  'precombine.field' = 'update_time',
  -- compaction
  'compaction.schedule.enabled' = 'true',
  'compaction.trigger.strategy' = 'time_elapsed',
  'compaction.delta_seconds' = '1800',
  'compaction.async.enabled' = 'false',
  'clean.async.enabled' = 'false'
)