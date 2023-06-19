package com.liang.spark.job;

import com.liang.spark.basic.SparkSessionFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySQLToHiveJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            args = new String[]{"mysql-to-hive.yml"};
        }
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.read().option("fetchsize", "2048").jdbc(
                "jdbc:mysql://e1d4c0a1d8d1456ba4b461ab8b9f293din01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/prism_shareholder_path" +
                        //时区
                        "?serverTimezone=GMT%2B8" +
                        //时间字段处理
                        "&zeroDateTimeBehavior=convertToNull" +
                        //编码
                        "&useUnicode=true" +
                        "&characterEncoding=UTF-8" +
                        "&characterSetResults=UTF-8" +
                        //useSSL
                        "&useSSL=false" +
                        //连接策略
                        "&autoReconnect=true" +
                        "&maxReconnects=3" +
                        "&failOverReadOnly=false" +
                        //性能优化
                        "&maxAllowedPacket=67108864" + //64mb
                        "&rewriteBatchedStatements=true",
                "no_shareholder_company_info",
                "id",
                108L,
                88888888888L,
                200,
                new Properties() {{
                    put("user", "jdhw_d_data_dml");
                    put("password", "2s0^tFa4SLrp72");
                    put("driver", "com.mysql.jdbc.Driver");
                }}
        ).write().mode(SaveMode.Overwrite).saveAsTable("test.no_shareholder_company_info");

        spark.stop();
    }
}
