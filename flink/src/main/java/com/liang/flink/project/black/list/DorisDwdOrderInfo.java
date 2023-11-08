package com.liang.flink.project.black.list;

import com.liang.common.dto.DorisOneRow;
import com.liang.common.dto.DorisSchema;
import com.liang.common.util.DateTimeUtils;

public class DorisDwdOrderInfo {
    public static DorisOneRow get() {
        String datetime = DateTimeUtils.fromUnixTime(System.currentTimeMillis() / 1000 + 1800);
        DorisSchema schema = DorisSchema.builder()
                .database("dwd")
                .tableName("dwd_order_info")
                .build();
        return new DorisOneRow(schema)
                .put("order_id", "d3c1bae2c5c740b3ba6ff2255eb158c9")
                .put("order_code", "O202203201505420923153416211")
                .put("tyc_user_id", "2768")
                .put("mobile", "15801005792")
                .put("sku_id", "11")
                .put("vip_from_time", "2018-05-28 20:52:00")
                .put("vip_to_time", "2021-02-01 09:37:26")
                .put("order_status", "1")
                .put("amount", "0")
                .put("actual_amount", "0")
                .put("invite_code", "")
                .put("pay_date", "NULL")
                .put("create_date", datetime.substring(0, 10))
                .put("platform_name", "Other")
                .put("pay_way", "其他")
                .put("pay_point_id", "")
                .put("create_time", datetime)
                .put("update_time", datetime);
    }
}
