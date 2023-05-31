package com.liang.spark.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CountDistinct extends UserDefinedAggregateFunction {
    public static byte[] serialize(Serializable obj) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
        ) {
            if (obj == null) {
                return null;
            }
            objectOutputStream.writeObject(obj);
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            return null;
        }
    }

    public static Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)
        ) {
            return objectInputStream.readObject();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public StructType inputSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("input", DataTypes.LongType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public StructType bufferSchema() {
        List<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("buffer", DataTypes.BinaryType, true));
        return DataTypes.createStructType(structFields);
    }

    @Override
    public DataType dataType() {
        return DataTypes.LongType;
    }

    @Override
    public boolean deterministic() {
        return true;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, serialize(new Roaring64Bitmap()));
    }

    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        byte[] buf = (byte[]) buffer.get(0);
        Long in = (Long) input.get(0);
        Roaring64Bitmap bitmap = (Roaring64Bitmap) deserialize(buf);
        bitmap.add(in);
        buffer.update(0, serialize(bitmap));

    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        byte[] buf1 = (byte[]) buffer1.get(0);
        byte[] buf2 = (byte[]) buffer2.get(0);
        Roaring64Bitmap bitmap1 = (Roaring64Bitmap) deserialize(buf1);
        Roaring64Bitmap bitmap2 = (Roaring64Bitmap) deserialize(buf2);
        bitmap1.or(bitmap2);
        buffer1.update(0, serialize(bitmap1));
    }

    @Override
    public Object evaluate(Row buffer) {
        Roaring64Bitmap bitmap = (Roaring64Bitmap) deserialize((byte[]) buffer.get(0));
        return bitmap.getLongCardinality();
    }
}
