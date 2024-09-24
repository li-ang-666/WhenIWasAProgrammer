package com.liang.common.service;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

/**
 * @author Clinton Begin
 */
@JsonSerialize(using = ToStringSerializer.class)
public class SQL extends AbstractSQL<SQL> {
    @Override
    public SQL getSelf() {
        return this;
    }
}
