package com.liang.spark.function;

import com.liang.common.dto.HbaseOneRow;
import org.apache.spark.sql.Row;

import java.io.Serializable;

@FunctionalInterface
public interface RowMapper extends Serializable {
    HbaseOneRow map(Row row);
}
