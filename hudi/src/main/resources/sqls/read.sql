create table enterprise(
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
  'path' = 'obs://hadoop-obs/hudi_ods/ratio_path_company004',
  'table.type' = 'MERGE_ON_READ',
  -- cdc
  'changelog.enabled' = 'true',
  -- read
  'read.tasks' = '1',
  'read.streaming.enabled' = 'true',
  'read.start-commit' = 'earliest',
  'read.streaming.check-interval' = '5'
);