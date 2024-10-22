package com.liang.flink.test;

import com.liang.common.dto.DorisOneRow;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
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
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
public class FlinkTest {
    private static final int PRECISION = 23;
    private static final int SCALE = 3;

    public static void main(String[] args) throws Exception {
        Schema columnSchema;
        if (PRECISION > 18) {
            columnSchema = LogicalTypes.decimal(PRECISION, SCALE).addToSchema(Schema.create(Schema.Type.BYTES));
        } else {
            columnSchema = LogicalTypes.decimal(PRECISION, SCALE).addToSchema(Schema.createFixed("id", null, null, computeMinBytesForDecimalPrecision(PRECISION)));
        }
        Schema rowSchema = SchemaBuilder.record(DorisOneRow.class.getSimpleName()).fields()
                .name("id")
                .type(columnSchema)
                .withDefault(null)
                .endRecord();
        GenericData.Record record = new GenericRecordBuilder(rowSchema) {{
            set("id", new BigDecimal("123.456").setScale(SCALE, RoundingMode.DOWN));
        }}.build();


        ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path("obs://hadoop-obs/flink/test/" + UUID.randomUUID()))
                .withSchema(rowSchema)
                .withDataModel(new GenericData() {{
                    addLogicalTypeConversion(new Conversions.DecimalConversion());
                }})
                .build();
        writer.write(record);
        writer.close();
    }

    public static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }
}
