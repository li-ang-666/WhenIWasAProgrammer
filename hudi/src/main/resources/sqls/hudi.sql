create table hudi_table(
    id                         BIGINT,
    company_id                 BIGINT,
    shareholder_id             STRING,
    shareholder_entity_type    BIGINT,
    shareholder_name_id        BIGINT,
    investment_ratio_total     DECIMAL(24, 12),
    is_controller              BIGINT,
    is_ultimate                BIGINT,
    is_big_shareholder         BIGINT,
    is_controlling_shareholder BIGINT,
    equity_holding_path        STRING,
    create_time                TIMESTAMP(0),
    update_time                TIMESTAMP(0),
    is_deleted                 BIGINT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi/hudi_table',
  'table.type' = 'MERGE_ON_READ',
  -- compaction
  'compaction.schedule.enabled' = 'true',
  'compaction.async.enabled' = 'true',
  'compaction.trigger.strategy' = 'num_commits',
  'compaction.delta_commits' = '3',
  -- changelog
  'changelog.enabled' = 'true'
)