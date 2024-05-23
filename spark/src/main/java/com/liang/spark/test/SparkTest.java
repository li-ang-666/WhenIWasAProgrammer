package com.liang.spark.test;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.math.BigDecimal;
import java.util.HashMap;

@Slf4j
public class SparkTest {
    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record("ColumnMap").fields()
                .optionalString("id")
                .optionalString("name")
                .optionalString("age")
                .endRecord();
        HashMap<String, Object> columnMap = new HashMap<>();
        columnMap.put("id", 1);
        columnMap.put("name", "LA");
        columnMap.put("age", new BigDecimal("11"));
        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
        columnMap.forEach((k, v) -> genericRecordBuilder.set(k, StrUtil.toStringOrNull(v)));
        ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter.<GenericRecord>builder(new Path("obs://hadoop-obs/flink/test1/bbb.parquet"))
                .withCompressionCodec(CompressionCodecName.BROTLI)
                .withSchema(schema)
                .withRowGroupSize(1024 * 1024 * 2)
                .build();
        parquetWriter.write(genericRecordBuilder.build());
        parquetWriter.close();
    }
}
