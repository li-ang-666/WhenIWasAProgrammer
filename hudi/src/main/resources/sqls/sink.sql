CREATE TABLE flink_sql_sink(
  name string,
  cnt BIGINT,
  PRIMARY KEY (name) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/test',
  'table-name' = 'flink_sql_sink',
  'username' = 'jdtest_d_data_ddl',
  'password' = 'dwjIFAmM39Y2O98cKu'
);