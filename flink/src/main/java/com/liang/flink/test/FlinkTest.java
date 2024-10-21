package com.liang.flink.test;

import com.liang.common.dto.DorisOneRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        Schema schema = SchemaBuilder.record(DorisOneRow.class.getSimpleName()).fields()
                .name("id").type(LogicalTypes.decimal(32, 12).addToSchema(SchemaBuilder.builder().bytesType())).withDefault(null)
                .name("name").type(SchemaBuilder.builder().stringType()).withDefault(null)
                .endRecord();
        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(null);
        GenericRecordBuilder record = new GenericRecordBuilder(schema);
        record.set("id", new BigDecimal("100.001"));
        record.set("name", "liang");
        record.build();
        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path("obs://hadoop-obs/flink/test/" + UUID.randomUUID()))
                .withDataModel(genericData)
                .withSchema(schema)
                .build();
        writer.write(record.build());
        writer.close();
    }
}
