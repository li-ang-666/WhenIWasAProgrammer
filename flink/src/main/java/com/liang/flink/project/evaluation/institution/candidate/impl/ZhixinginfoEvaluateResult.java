package com.liang.flink.project.evaluation.institution.candidate.impl;

import com.liang.common.service.SQL;
import com.liang.common.util.SqlUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.project.evaluation.institution.candidate.CaseCodeClean;
import com.liang.flink.project.evaluation.institution.candidate.EvaluationInstitutionCandidateService;
import com.liang.flink.service.data.update.AbstractDataUpdate;
import com.obs.services.ObsClient;
import com.obs.services.model.ObsObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ZhixinginfoEvaluateResult extends AbstractDataUpdate<String> {
    private final CaseCodeClean caseCodeClean = new CaseCodeClean();
    // obs
    private final String endPoint = "obs.cn-north-4.myhuaweicloud.com";
    private final String ak = "NT5EWZ4FRH54R2R2CB8G";
    private final String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final ObsClient obsClient = new ObsClient(ak, sk, endPoint);
    private int i = 0;

    @Override
    public List<String> updateWithReturn(SingleCanalBinlog singleCanalBinlog) {
        System.out.println(++i);
        List<String> sqls = new ArrayList<>();
        Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
        String referenceMode = String.valueOf(columnMap.get("reference_mode"));
        String deleted = String.valueOf(columnMap.get("deleted"));
        String index = String.valueOf(columnMap.get("result_text_oss"));
        if (!referenceMode.matches("委托评估|定向询价") || !"0".equals(deleted) || !index.startsWith("pan_zhixing/xunjiapinggu_result_content/")) {
            return sqls;
        }
        // company
        String companyName;
        try {
            ObsObject obsObject = obsClient.getObject("jindi-oss-wangsu", index);
            InputStream input = obsObject.getObjectContent();
            String content = IOUtils.toString(input, StandardCharsets.UTF_8);
            if ("委托评估".equals(referenceMode)) {
                companyName = content.replaceAll("\\s", "").replaceAll(".*委托(.*?)进行评估.*", "$1");
            } else {
                companyName = content.replaceAll("\\s", "").replaceAll(".*向(.*?)发出定向询价函.*", "$1");
            }
        } catch (Exception e) {
            log.error("查询obs异常, objectKey = {}", index, e);
            return sqls;
        }
        if (companyName.length() > 255) {
            return sqls;
        }
        // case number
        String caseNumber = String.valueOf(columnMap.get("caseNumber"));
        String caseNumberClean = caseCodeClean.evaluate(caseNumber);
        // subject name
        String subjectName = String.valueOf(columnMap.get("subjectName")).replaceAll("\\s", "");
        // return sql
        String sql = new SQL().UPDATE(EvaluationInstitutionCandidateService.TABLE)
                .SET("is_eventual_evaluation_institution = 1")
                .WHERE("entity_name_valid_selected_evaluation_institution = " + SqlUtils.formatValue(companyName))
                .WHERE("enforcement_object_name = " + SqlUtils.formatValue(subjectName))
                .WHERE("enforcement_case_number = " + SqlUtils.formatValue(caseNumberClean))
                .toString();
        sqls.add(sql);
        return sqls;
    }

    @Override
    public List<String> deleteWithReturn(SingleCanalBinlog singleCanalBinlog) {
        return updateWithReturn(singleCanalBinlog);
    }
}
