package com.liang.common.service.database.template.inner;

import java.io.Serializable;
import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetMapper<T> extends Serializable {
    T map(ResultSet rs) throws Exception;
}
