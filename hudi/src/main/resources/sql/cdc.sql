CREATE TABLE ods (%s
  op_ts TIMESTAMP(3) METADATA FROM 'op_ts' VIRTUAL,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '%s',
  'port' = '3306',
  'username' = 'jdhw_d_data_dml',
  'password' = '2s0^tFa4SLrp72',
  'database-name' = '%s',
  'table-name' = '%s',
  'server-id' = '%s',
  'scan.startup.mode' = 'earliest-offset',
  'server-time-zone' = 'Asia/Shanghai',
  'jdbc.properties.useSSL' = 'false',
  'jdbc.properties.zeroDateTimeBehavior' = 'convertToNull',
  'jdbc.properties.tinyInt1isBit' = 'false'
);
CREATE TABLE dwd (%s
  op_ts TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_ods/%s',
  'table.type' = 'MERGE_ON_READ',
  -- index
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.num.buckets' = '32',
  -- write
  'write.tasks' = '2',
  'write.task.max.size' = '2048',
  -- compaction
  'compaction.async.enabled' = 'false',
  'compaction.schedule.enabled' = 'true',
  'compaction.trigger.strategy' = 'num_or_time',
  'compaction.delta_commits' = '3',
  'compaction.delta_seconds' = '3600',
  -- hive
  'hive_sync.enabled' = 'true',
  'hive_sync.mode' = 'hms',
  'hive_sync.metastore.uris' = 'thrift://10.99.202.153:9083,thrift://10.99.198.86:9083',
  'hive_sync.db' = 'hudi_ods',
  'hive_sync.table' = '%s',
  'hive_sync.table.strategy' = 'RO',
  'hive_sync.skip_ro_suffix' = 'true'
);
%s;