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
  'server-id' = '6000-6127',
  'scan.startup.mode' = 'earliest-offset'
);
CREATE TABLE dwd(%s
  op_ts TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'obs://hadoop-obs/hudi_ods/%s',
  'table.type' = 'MERGE_ON_READ',
  -- index
  'index.type' = 'BUCKET',
  'hoodie.bucket.index.num.buckets' = '128',
  -- write
  'write.tasks' = '4',
  'write.task.max.size' = '512',
  'write.merge.max_memory' = '0',
  'write.precombine' = 'true',
  'write.precombine.field' = 'op_ts',
  'changelog.enabled' = 'true',
  -- compaction
  'compaction.async.enabled' = 'false',
  'compaction.delta_commits' = '30',
  -- clean & archive
  'clean.retain_commits' = '3000',
  'archive.min_commits' = '3010',
  'archive.max_commits' = '3020',
  -- hive
  'hive_sync.enabled' = 'true',
  'hive_sync.mode' = 'hms',
  'hive_sync.metastore.uris' = 'thrift://10.99.202.153:9083',
  'hive_sync.db' = 'hudi_ods',
  'hive_sync.table' = '%s',
  'hive_sync.table.strategy' = 'RO',
  'hive_sync.skip_ro_suffix' = 'true'
);
%s;