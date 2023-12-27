package com.liang.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;

@Description(
        name = "_FUNC_",
        value = "_FUNC_(int[] / string[]) - if string[], do concat; if int[], do plus",
        extended = "Example:" +
                "\n  jdbc:hive2://127.0.0.1:10000/> select _FUNC_('a', 'b', 'c');" +
                "\n  jdbc:hive2://127.0.0.1:10000/> abc" +
                "\n  jdbc:hive2://127.0.0.1:10000/> select _FUNC_(111, 222, 333);" +
                "\n  jdbc:hive2://127.0.0.1:10000/> 666"
)
public class DemoUDF extends UDF {
    public String evaluate(String... strArr) {
        return String.join("", strArr);
    }

    public Integer evaluate(Integer... intArr) {
        return Arrays.stream(intArr).mapToInt(e -> e).sum();
    }
}
