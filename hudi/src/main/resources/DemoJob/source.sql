CREATE TABLE enterprise (
  `topic` STRING METADATA VIRTUAL,
  `partition` BIGINT METADATA VIRTUAL,
  `offset` BIGINT METADATA VIRTUAL,
  `kafka_time` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL,
  `id` BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '56736.json.prism.enterprise',
  'properties.bootstrap.servers' = '10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092',
  'properties.group.id' = 'demo-job',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'canal-json',
  'canal-json.ignore-parse-errors' = 'true',
  'canal-json.encode.decimal-as-plain-number' = 'true'
)