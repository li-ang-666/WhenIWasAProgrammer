package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.*;

/**
 * Author: phoenix。
 * Date: 2023/12/12 18:56。
 * FileName: CompanyCleanInfoPatchJob。
 * Description: 。
 * 需求文档：
 * 探查文档：
 * 开发文档：
 */
@LocalConfigFile("company-clean-info-patch.yml")
public class CompanyCleanInfoPatchJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(null);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = StreamFactory.create(env);
        stream.rebalance()
                .addSink(new CompanyCleanInfoPatchSink(config)).setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("CompanyCleanInfoPatchJob");
    }

    @RequiredArgsConstructor
    @Slf4j
    private final static class CompanyCleanInfoPatchSink extends RichSinkFunction<SingleCanalBinlog> {
        private final Config config;
        private JdbcTemplate jdbcTemplate;

        //todo 传入社保信息表的5个人数字段，添加到list集合中
        private static List<Integer> regexp(Object endowmentInsurance, List<Integer> list) {
            if (endowmentInsurance == null) {
                list.add(0);
            } else {
                try {
                    list.add(Integer.parseInt(endowmentInsurance.toString().replace("人", "").trim()));
                } catch (Exception e) {
                    list.add(0);
                }
            }
            return list;
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            jdbcTemplate = new JdbcTemplate("116.prism");
        }

        @Override
        public void invoke(SingleCanalBinlog binlog, Context context) {
            Map<String, Object> beforeColumnMap = binlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = binlog.getAfterColumnMap();
            if (!Objects.equals(beforeColumnMap.get("social_security_staff_num"), afterColumnMap.get("social_security_staff_num"))) {
                log.info("before: {}, after: {}", beforeColumnMap.get("social_security_staff_num"), afterColumnMap.get("social_security_staff_num"));

                Object companyId = afterColumnMap.get("id");
                String sql = new SQL().SELECT("id")
                        .FROM("annual_report")
                        .WHERE("company_id = " + SqlUtils.formatValue(companyId))
                        .ORDER_BY("report_year desc")
                        .LIMIT(1)
                        .toString();
                Long l = jdbcTemplate.queryForObject(sql, rs -> rs.getLong(1));
                if (l != null) {
                    String sql1 = new SQL().SELECT("endowment_insurance"
                                    , "unemployment_insurance"
                                    , "medical_insurance"
                                    , "employment_injury_insurance"
                                    , "maternity_insurance")
                            .FROM("report_social_security_info")
                            .WHERE("annaulreport_id = " + SqlUtils.formatValue(l))
                            .LIMIT(1)
                            .toString();
                    Tuple5<String, String, String, String, String> tuple5 = jdbcTemplate.queryForObject(sql1, rs -> {
                        String s1 = rs.getString(1);
                        String s2 = rs.getString(2);
                        String s3 = rs.getString(3);
                        String s4 = rs.getString(4);
                        String s5 = rs.getString(5);
                        return new Tuple5<String, String, String, String, String>().of(s1, s2, s3, s4, s5);
                    });
                    if (tuple5 != null) {

                        ArrayList<Integer> list = new ArrayList<>();
                        regexp(tuple5.f0, list);
                        regexp(tuple5.f1, list);
                        regexp(tuple5.f2, list);
                        regexp(tuple5.f3, list);
                        regexp(tuple5.f4, list);
                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer o1, Integer o2) {
                                return o2 - o1;
                            }
                        });
                        Integer resultNum = list.get(0);
                        if (!Objects.equals(String.valueOf(resultNum), afterColumnMap.get("social_security_staff_num"))) {
                            jdbcTemplate.update(String.format("update company_clean_info set social_security_staff_num = %s where id = %s", resultNum, SqlUtils.formatValue(companyId)));
                        }

                    }

                }


            }
        }
    }
}
