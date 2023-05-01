package com.liang.common.service.database.template;

import java.io.Serializable;
import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetMapper<T> extends Serializable {
    T map(ResultSet rs) throws Exception;
}
