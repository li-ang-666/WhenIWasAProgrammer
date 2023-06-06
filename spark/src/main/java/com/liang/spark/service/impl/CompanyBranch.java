package com.liang.spark.service.impl;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.dao.CompanyBranchSqlHolder;
import com.liang.spark.job.DataConcatJob;
import com.liang.spark.service.AbstractSparkRunner;
import org.apache.spark.sql.SparkSession;

public class CompanyBranch extends AbstractSparkRunner {
    private final CompanyBranchSqlHolder sqlHolder = new CompanyBranchSqlHolder();

    @Override
    public void run(SparkSession spark) {
        spark.sql(preQuery(sqlHolder.queryTotalBranch()))
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow("dataConcatCompanyBaseSchema", companyId)
                            .put("company_branch_total_branch", value);
                }));
        spark.sql(preQuery(sqlHolder.queryTotalCanceledBranch()))
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow("dataConcatCompanyBaseSchema", companyId)
                            .put("company_branch_total_canceled_branch", value);
                }));
        spark.sql(preQuery(sqlHolder.queryMostYear()))
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow("dataConcatCompanyBaseSchema", companyId)
                            .put("company_branch_most_year", value);
                }));
        spark.sql(preQuery(sqlHolder.queryMostArea()))
                .foreachPartition(new DataConcatJob.HbaseSink(ConfigUtils.getConfig(), null, (isHistory, row) -> {
                    String companyId = String.valueOf(row.get(0));
                    String value = String.valueOf(row.get(1));
                    return new HbaseOneRow("dataConcatCompanyBaseSchema", companyId)
                            .put("company_branch_most_area", value);
                }));
    }
}
