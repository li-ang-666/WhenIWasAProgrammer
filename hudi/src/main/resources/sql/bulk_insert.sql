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
  'scan.partition.num' = '1',
  'scan.fetch-size' = '-2147483648'
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
  'write.operation' = 'bulk_insert',
  'write.bulk_insert.shuffle_input' = 'false',
  'write.bulk_insert.sort_input' = 'false',
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