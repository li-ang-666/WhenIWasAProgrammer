CREATE TABLE ods (
  id decimal(20,0),
  name string,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '9349c027b3b4414aa5f9019cd218e7a3in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com',
  'port' = '3306',
  'username' = 'canal_d',
  'password' = 'Canal@Dduan',
  'database-name' = 'test',
  'table-name' = 'test',
  'server-id' = '8888',
  'scan.startup.mode' = 'earliest-offset',
  'server-time-zone' = 'Asia/Shanghai',
  'jdbc.properties.useSSL' = 'false',
  'jdbc.properties.zeroDateTimeBehavior' = 'convertToNull',
  'jdbc.properties.tinyInt1isBit' = 'false'
);
CREATE TABLE dwd (
  id decimal(20,0),
  name string
) WITH (
  'connector' = 'kafka',
  'topic' = 'abc',
  'properties.bootstrap.servers' = '10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092',
  'format' = 'json'
);
insert into dwd select * from ods;