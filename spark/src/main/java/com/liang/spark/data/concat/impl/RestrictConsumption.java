package com.liang.spark.data.concat.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.data.concat.dao.RestrictConsumptionSqlHolder;
import com.liang.spark.job.DataConcatJob;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class RestrictConsumption {
    private final RestrictConsumptionSqlHolder sqlHolder = new RestrictConsumptionSqlHolder();

    public void run(SparkSession spark) {
        HbaseSchema hbaseSchemaHis = new HbaseSchema("prism_c", "historical_info_splice", "ds", true);
        HbaseSchema hbaseSchemaJudi = new HbaseSchema("prism_c", "judicial_risk_splice", "ds", true);
        for (Boolean bool : Arrays.asList(true, false)) {
            spark.sql(sqlHolder.queryMostApplicant(bool))
                    .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), bool, (isHistory, row) -> {
                        String companyId = String.valueOf(row.get(0));
                        String concat = String.valueOf(row.get(1));
                        String type;
                        String id;
                        String name;
                        if (concat.matches(".*?:\\d+")) {
                            type = "1";
                            String[] split = concat.split(":");
                            id = split[1];
                            name = split[0].split("、")[0];
                        } else {
                            type = null;
                            id = null;
                            name = concat.split("、")[0];
                        }
                        return new HbaseOneRow(isHistory ? hbaseSchemaHis : hbaseSchemaJudi, companyId)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_applicant_type", type)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_applicant_id", id)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_applicant_name", name);
                    }));
            spark.sql(sqlHolder.queryMostRelatedRestricted(bool))
                    .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), bool, (isHistory, row) -> {
                        String companyId = String.valueOf(row.get(0));
                        String concat = String.valueOf(row.get(1));
                        String type;
                        String id;
                        String name;
                        if (concat.matches("(.*?):(\\d+):company")) {
                            type = "1";
                            id = concat.replaceAll("(.*?):(\\d+):company", "$2");
                            name = concat.replaceAll("(.*?):(\\d+):company", "$1");
                        } else if (concat.matches("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)")) {
                            type = "2";
                            id = concat.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$3");
                            name = concat.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$1");
                        } else {
                            type = null;
                            id = null;
                            name = concat.split("、")[0];
                        }
                        return new HbaseOneRow(isHistory ? hbaseSchemaHis : hbaseSchemaJudi, companyId)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_related_restricted_type", type)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_related_restricted_id", id)
                                .put((isHistory ? "history_" : "") + "restrict_consumption_most_related_restricted_name", name);
                    }));

            spark.sql(sqlHolder.queryMostRestrivted(bool))
                    .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), bool, (isHistory, row) -> {
                        {
                            String companyId = String.valueOf(row.get(0));
                            String concat = String.valueOf(row.get(1));
                            String type;
                            String id;
                            String name;
                            if (concat.matches("(.*?):(\\d+):company")) {
                                type = "1";
                                id = concat.replaceAll("(.*?):(\\d+):company", "$2");
                                name = concat.replaceAll("(.*?):(\\d+):company", "$1");
                            } else if (concat.matches("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)")) {
                                type = "2";
                                id = concat.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$3");
                                name = concat.replaceAll("(.*?):([A-Z0-9]+):(\\d+)-(\\d+):(human)", "$1");
                            } else {
                                type = null;
                                id = null;
                                name = concat.split("、")[0];
                            }
                            return new HbaseOneRow(isHistory ? hbaseSchemaHis : hbaseSchemaJudi, companyId)
                                    .put((isHistory ? "history_" : "") + "restrict_consumption_most_restricted_type", type)
                                    .put((isHistory ? "history_" : "") + "restrict_consumption_most_restricted_id", id)
                                    .put((isHistory ? "history_" : "") + "restrict_consumption_most_restricted_name", name);
                        }
                    }));
        }
    }
}
