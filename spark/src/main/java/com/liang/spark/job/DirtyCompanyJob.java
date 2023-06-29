package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class DirtyCompanyJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);

        spark.sql("use test");
        spark.sql("select distinct name from ods.ods_prism1_company_df where pt='20230628'")
                .createOrReplaceTempView("t1");
        spark.sql("select distinct dirty_company from test.test_dirty_company_df")
                .createOrReplaceTempView("t2");
        spark.sql("insert overwrite table test.test_dirty_company_df_result_copy_new partition(pt='20230628')" +
                " select /*+ REPARTITION(1200) */ name from t1 left semi join t2" +
                " on substr(reverse(name),0,1) = substr(reverse(dirty_company),0,1) and substr(reverse(name),0,length(dirty_company)) = reverse(dirty_company)");
//        spark.sql("insert overwrite table test.test_dirty_company_df_result_copy_new partition(pt='20230627')\n" +
//                "select /*+ REPARTITION(600) */ name from\n" +
//                "(select name from ods.ods_prism1_company_df where pt='20230627' group by name)t1\n" +
//                "join\n" +
//                "(select dirty_company from test.test_dirty_company_df)t2\n" +
//                "on t1.name  LIKE CONCAT('%', t2.dirty_company)");
        spark.stop();
    }
}
