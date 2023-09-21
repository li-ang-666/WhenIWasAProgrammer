CREATE TABLE cdc_table (
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
  -- common
  'connector' = 'mysql-cdc',
  'hostname' = 'e1d4c0a1d8d1456ba4b461ab8b9f293din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com',
  'port' = '3306',
  'username' = 'jdhw_d_data_dml',
  'password' = '2s0^tFa4SLrp72',
  'database-name' = 'prism_shareholder_path',
  'table-name' = 'ratio_path_company',
  'server-id' = '6000-6127',
  'server-time-zone' = 'Asia/Shanghai',
  'connect.timeout' = '60s',
  'connect.max-retries' = '5',
  'heartbeat.interval' = '600s',
  -- scan
  'scan.incremental.snapshot.chunk.size' = '10240',
  'scan.snapshot.fetch.size' = '2048',
  -- debezium
  'debezium.min.row.count.to.stream.result' = '0'
);