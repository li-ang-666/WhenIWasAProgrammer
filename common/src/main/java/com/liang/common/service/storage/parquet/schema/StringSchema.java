package com.liang.common.service.storage.parquet.schema;

import org.apache.avro.Schema;

public class StringSchema extends ReadableSchema {
    public StringSchema(String name) {
        super(name);
    }

    @Override
    public String getSqlType() {
        return "STRING";
    }

    @Override
    public Schema getSchema() {
        return Schema.create(Schema.Type.STRING);
    }

    @Override
    public Object formatValue(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }
}