CREATE TABLE ods (%s
  op_ts as CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://%s:3306/%s?useSSL=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false',
  'table-name' = '%s',
  'username' = 'jdhw_d_data_dml',
  'password' = '2s0^tFa4SLrp72',
  'scan.partition.column' = 'id',
  'scan.partition.lower-bound' = '%s',
  'scan.partition.upper-bound' = '%s',
  'scan.partition.num' = '%s',
  'scan.fetch-size' = '1024'
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
  'write.tasks' = '32',
  'write.task.max.size' = '1024',
  'write.merge.max_memory' = '0',
  'write.precombine' = 'true',
  'write.precombine.field' = 'op_ts',
  'write.operation' = 'bulk_insert',
  'write.bulk_insert.shuffle_input' = 'false',
  'write.bulk_insert.sort_input' = 'false',
  -- compaction
  'compaction.async.enabled' = 'false',
  'compaction.schedule.enabled' = 'true',
  'compaction.trigger.strategy' = 'num_or_time',
  'compaction.delta_commits' = '3',
  'compaction.delta_seconds' = '3600',
  -- clean & archive
  'clean.async.enabled' = 'true',
  'clean.retain_commits' = '10',
  'archive.min_commits' = '20',
  'archive.max_commits' = '30',
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