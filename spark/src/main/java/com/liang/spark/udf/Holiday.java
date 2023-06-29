package com.liang.spark.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class Holiday extends UDF {
    public String evaluate(String dt) {
        return "aaaa";
    }
}
