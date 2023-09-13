--CREATE TABLE cdc_table (
--  id                         BIGINT,
--  company_id                 BIGINT,
--  shareholder_id             STRING,
--  shareholder_entity_type    BIGINT,
--  shareholder_name_id        BIGINT,
--  investment_ratio_total     DECIMAL(24, 12),
--  is_controller              BIGINT,
--  is_ultimate                BIGINT,
--  is_big_shareholder         BIGINT,
--  is_controlling_shareholder BIGINT,
--  equity_holding_path        STRING,
--  create_time                TIMESTAMP(0),
--  update_time                TIMESTAMP(0),
--  is_deleted                 BIGINT,
--  PRIMARY KEY (id) NOT ENFORCED
--) WITH (
--  'connector' = 'mysql-cdc',
--  'hostname' = 'e1d4c0a1d8d1456ba4b461ab8b9f293din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com',
--  'port' = '3306',
--  'username' = 'jdhw_d_data_dml',
--  'password' = '2s0^tFa4SLrp72',
--  'database-name' = 'prism_shareholder_path',
--  'table-name' = 'ratio_path_company',
--  'server-id' = '6000-6010',
--  'server-time-zone' = 'Asia/Shanghai',
--  'scan.startup.mode' = 'earliest-offset'
--);


CREATE TABLE cdc_table (
  id BIGINT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com',
  'port' = '3306',
  'username' = 'jdtest_d_data_ddl',
  'password' = 'dwjIFAmM39Y2O98cKu',
  'database-name' = 'test',
  'table-name' = 'flink_sql_test',
  'server-id' = '6000-6010',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'latest-offset'
);