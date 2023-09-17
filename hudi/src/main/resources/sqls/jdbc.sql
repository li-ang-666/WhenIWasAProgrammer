CREATE TABLE jdbc_table(
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
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://e1d4c0a1d8d1456ba4b461ab8b9f293din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/prism_shareholder_path?useSSL=false',
  'table-name' = 'ratio_path_company',
  'username' = 'jdhw_d_data_dml',
  'password' = '2s0^tFa4SLrp72',
  'scan.partition.column' = 'id',
  'scan.partition.num' = '40000',
  'scan.partition.lower-bound' = '1',
  'scan.partition.upper-bound' = '400000000',
  'scan.fetch-size' = '1024'
);