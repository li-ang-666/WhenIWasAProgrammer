package com.liang.repair.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.repair.trait.Runner;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.List;

@Slf4j
public class ReadHbase implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        HbaseTemplate hbaseTemplate = new HbaseTemplate("hbaseSink");
        List<Tuple4<String, String, String, String>> row = hbaseTemplate.getRow(
                new HbaseOneRow("dataConcatJudicialRiskSchema", "14427175"));
        for (Tuple4<String, String, String, String> tuple4 : row) {
            log.info("{}:{} -> {}, {}", tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3);
        }

        List<Tuple4<String, String, String, String>> row1 = hbaseTemplate.getRow(
                new HbaseOneRow("dataConcatOperatingRiskSchema", "14427175"));
        for (Tuple4<String, String, String, String> tuple4 : row1) {
            log.info("{}:{} -> {}, {}", tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3);
        }

        List<Tuple4<String, String, String, String>> row2 = hbaseTemplate.getRow(
                new HbaseOneRow("dataConcatHistoricalInfoSchema", "14427175"));
        for (Tuple4<String, String, String, String> tuple4 : row2) {
            log.info("{}:{} -> {}, {}", tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3);
        }

        List<Tuple4<String, String, String, String>> row3 = hbaseTemplate.getRow(
                new HbaseOneRow("dataConcatCompanyBaseSchema", "14427175"));
        for (Tuple4<String, String, String, String> tuple4 : row3) {
            log.info("{}:{} -> {}, {}", tuple4.f0, tuple4.f1, tuple4.f2, tuple4.f3);
        }
    }
}
