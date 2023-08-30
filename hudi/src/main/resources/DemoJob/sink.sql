create table hudi_table(
    id                         BIGINT,
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
) with (
  'connector' = 'hudi',
  'path' = 'hdfs:///liang/hudi_table/',
  'table.type' = 'MERGE_ON_READ',
  -- read
  'read.streaming.enabled' = 'true',
  'read.streaming.start-commit' = '20230830010101',
  'read.streaming.check-interval' = '4'
)