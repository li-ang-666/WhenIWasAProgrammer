package com.liang.spark.data.concat.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.data.concat.dao.CompanyBranchSqlHolder;
import com.liang.spark.job.DataConcatJob;
import org.apache.spark.sql.SparkSession;

public class CompanyBranch {
    private final CompanyBranchSqlHolder sqlHolder = new CompanyBranchSqlHolder();

    public void run(SparkSession spark) {
        HbaseSchema hbaseSchema = new HbaseSchema("prism_c", "company_base_splice", "ds", true);
        spark.sql(sqlHolder.queryTotalBranch())
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow(hbaseSchema, companyId)
                            .put("company_branch_total_branch", value);
                }));
        spark.sql(sqlHolder.queryTotalCanceledBranch())
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow(hbaseSchema, companyId)
                            .put("company_branch_total_canceled_branch", value);
                }));
        spark.sql(sqlHolder.queryMostYear())
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow(hbaseSchema, companyId)
                            .put("company_branch_most_year", value);
                }));
        spark.sql(sqlHolder.queryMostArea())
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow(hbaseSchema, companyId)
                            .put("company_branch_most_area", value);
                }));
    }
}
