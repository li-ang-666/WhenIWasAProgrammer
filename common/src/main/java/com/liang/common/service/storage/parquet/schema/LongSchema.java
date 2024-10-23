package com.liang.common.service.storage.parquet.schema;

import lombok.experimental.PackagePrivate;
import org.apache.avro.Schema;

@PackagePrivate
class LongSchema extends ReadableSchema {
    @Override
    public String getSqlType() {
        return "BIGINT";
    }

    @Override
    public Schema getSchema() {
        return Schema.create(Schema.Type.LONG);
    }

    @Override
    public Object formatValue(Object value) {
        if (value == null) {
            return null;
        }
        return Long.parseLong(String.valueOf(value));
    }
}
