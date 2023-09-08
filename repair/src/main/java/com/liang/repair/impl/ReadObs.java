package com.liang.repair.impl;

import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import com.obs.services.ObsClient;
import com.obs.services.model.ObsObject;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ReadObs extends ConfigHolder {
    private final static String endPoint = "obs.cn-north-4.myhuaweicloud.com";
    private final static String ak = "NT5EWZ4FRH54R2R2CB8G";
    private final static String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";

    public static void main(String[] args) throws Exception {
        ObsClient obsClient = new ObsClient(ak, sk, endPoint);
        JdbcTemplate jdbcTemplate = new JdbcTemplate("108.data_judicial_risk");
        String sql = new SQL().SELECT("*")
                .FROM("zhixinginfo_evaluate_result")
                .WHERE("reference_mode in ('委托评估','定向询价')")
                .WHERE("deleted = 0")
                .WHERE("caseNumber is not null and caseNumber <> ''")
                .WHERE("subjectName is not null and subjectName <> ''")
                .WHERE("result_text_oss is not null and result_text_oss <>''")
                .toString();
        List<Map<String, Object>> columnMaps = jdbcTemplate.queryForColumnMaps(sql);
        for (Map<String, Object> columnMap : columnMaps) {
            String id = String.valueOf(columnMap.get("id"));
            String referenceMode = String.valueOf(columnMap.get("reference_mode"));
            String resultTextOss = String.valueOf(columnMap.get("result_text_oss"));
            ObsObject obsObject = obsClient.getObject("jindi-oss-wangsu", resultTextOss);
            InputStream input = obsObject.getObjectContent();
            String content = IOUtils.toString(input, StandardCharsets.UTF_8);
            String companyName;
            if ("委托评估".equals(referenceMode)) {
                companyName = content.replaceAll("\\s", "").replaceAll(".*委托(.*?)进行评估.*", "$1");
            } else {
                companyName = content.replaceAll("\\s", "").replaceAll(".*向(.*?)发出定向询价函.*", "$1");
            }
            log.info("{} -> {}", id, companyName);
        }
    }
}
