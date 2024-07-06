package com.liang.spark.job;

import org.roaringbitmap.longlong.Roaring64Bitmap;

public class CheckJob {
    public static void main(String[] args) {
//        SparkSession spark = SparkSessionFactory.createSpark(args);
//        TableFactory.jdbc(spark,"463.bdp_equity","shareholder_investment_ratio_total")
//                .createOrReplaceTempView("t");
//        spark.sql("insert overwrite table test.shareholder_investment_ratio_total select id,company_id,shareholder_id from t");

        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        bitmap.add(1);
        bitmap.add(2);
        bitmap.add(3);
        System.out.println(bitmap.stream().max());
    }
}
