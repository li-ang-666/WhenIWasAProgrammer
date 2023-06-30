package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SparkSession;

public class DirtyCompanyJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);

        spark.sql("use test");
        spark.sql("select distinct name,reverse(name) re,substr(reverse(name),0,1) head from ods.ods_prism1_company_df where pt='20230629' where name is not null and name <> ''")
                .createOrReplaceTempView("t1");
        spark.sql("select distinct dirty_company,reverse(dirty_company) re,substr(reverse(dirty_company),0,1) head,length(dirty_company) len from test.test_dirty_company_df where dirty_company is not null and dirty_company <> ''")
                .repartition(600)
                .createOrReplaceTempView("t2");
        spark.sql("insert overwrite table test.test_dirty_company_df_result_copy_new partition(pt='20230629')" +
                " select /*+ REPARTITION(600) */ name from t1 left semi join t2" +
                " on t1.head = t2.head and substr(t1.re,0,t2.len) = t2.re");
//        spark.sql("insert overwrite table test.test_dirty_company_df_result_copy_new partition(pt='20230627')\n" +
//                "select /*+ REPARTITION(600) */ name from\n" +
//                "(select name from ods.ods_prism1_company_df where pt='20230627' group by name)t1\n" +
//                "join\n" +
//                "(select dirty_company from test.test_dirty_company_df)t2\n" +
//                "on t1.name  LIKE CONCAT('%', t2.dirty_company)");
        spark.stop();
    }
}
