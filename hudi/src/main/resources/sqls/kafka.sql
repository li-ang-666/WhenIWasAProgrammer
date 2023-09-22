CREATE TABLE source_table (
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
  'connector' = 'kafka',
  'topic' = '9349c.json.prism_shareholder_path.ratio_path_company',
  'properties.bootstrap.servers' = '10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092',
  'properties.group.id' = 'demo-job',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'canal-json',
  'canal-json.ignore-parse-errors' = 'true',
  'canal-json.encode.decimal-as-plain-number' = 'true'
)