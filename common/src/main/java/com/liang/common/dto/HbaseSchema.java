package com.liang.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class HbaseSchema implements Serializable {
    // 人count
    public static final HbaseSchema HUMAN_ALL_COUNT = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("human_all_count")
            .columnFamily("cf")
            .rowKeyReverse(false)
            .build();
    // 公司count
    public static final HbaseSchema COMPANY_ALL_COUNT = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("company_all_count")
            .columnFamily("count")
            .rowKeyReverse(true)
            .build();
    // 公司背景
    public static final HbaseSchema COMPANY_BASE_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("company_base_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    // 司法风险
    public static final HbaseSchema JUDICIAL_RISK_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("judicial_risk_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    // 经营风险
    public static final HbaseSchema OPERATING_RISK_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("operating_risk_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    // 经营状况
    public static final HbaseSchema OPERATING_INFO_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("operating_info_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    // 知识产权
    public static final HbaseSchema INTELLECTUAL_PROPERTY_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("intellectual_property_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    // 历史信息
    public static final HbaseSchema HISTORICAL_INFO_SPLICE = HbaseSchema.builder()
            .namespace("prism_c")
            .tableName("historical_info_splice")
            .columnFamily("ds")
            .rowKeyReverse(true)
            .build();
    private String namespace;
    private String tableName;
    private String columnFamily;
    private boolean rowKeyReverse;
}
