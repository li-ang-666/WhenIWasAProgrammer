package com.liang.flink.project.black.list;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.util.DateTimeUtils;

public class DorisDwdAppActive {
    public static DorisOneRow get() {
        String datetime = DateTimeUtils.fromUnixTime(System.currentTimeMillis() / 1000 + 3600);
        DorisSchema schema = DorisSchema.builder()
                .database("dwd")
                .tableName("dwd_app_active")
                .build();
        return new DorisOneRow(schema)
                .put("app_id2", "29c72a2af7c16f32")
                .put("pt", "2023-10-09")
                .put("android_id", "29c72a2af7c16f32")
                .put("imei", "NULL")
                .put("oaid", "b3765e1436a52d6a")
                .put("idfa", "NULL")
                .put("idfv", "NULL")
                .put("type", "0")
                .put("umeng_channel", "MIUI")
                .put("create_time", datetime)
                .put("app_version", "Android 13.8.22")
                .put("update_time", datetime);
    }
}
