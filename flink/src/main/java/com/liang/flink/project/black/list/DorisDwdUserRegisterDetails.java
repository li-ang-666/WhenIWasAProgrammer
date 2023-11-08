package com.liang.flink.project.black.list;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.util.DateTimeUtils;

public class DorisDwdUserRegisterDetails {
    public static DorisOneRow get() {
        DorisSchema schema = DorisSchema.builder()
                .database("dwd")
                .tableName("dwd_user_register_details")
                .build();
        return new DorisOneRow(schema)
                .put("tyc_user_id", "623")
                .put("mobile", "15952876989")
                .put("register_time", DateTimeUtils.currentDatetime())
                .put("vip_from_time", "NULL")
                .put("vip_to_time", "NULL")
                .put("user_type", "2")
                .put("create_time", DateTimeUtils.currentDatetime())
                .put("update_time", DateTimeUtils.currentDatetime());
    }
}
