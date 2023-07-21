package org.tyc.entity;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 统一社会信用代码的枚举值
 */
public class TwoCreditCodeEnum {

    /**
     * 不作为大股东的起点的统一社会信用代码前两位
     */
    public static Set<String> ForBigShareHolder =
            new HashSet<>(Arrays.asList(
                    "31",
                    "91",
                    "92",
                    "93"
            ));

    /**
     * 不作为控股股东的起点的统一社会信用代码前两位
     */
    public static Set<String> ForBigControllingShareHolder =
            new HashSet<>(Arrays.asList(
                    "31",
                    "91",
                    "92",
                    "93"
            ));

    /**
     * 不作为实际控制人的起点的统一社会信用代码前两位
     */
    public static Set<String> ForContrller =
            new HashSet<>(Arrays.asList(
                    "31",
                    "91",
                    "92",
                    "93"
            ));

    /**
     * 不作为最终受益人的起点的统一社会信用代码前两位
     */
    public static Set<String> ForUltimater =
            new HashSet<>(Arrays.asList(
                    "31",
                    "51",
                    "91",
                    "92",
                    "93",
                    "J1",
                    "N1",
                    "N2",
                    "N3",
                    "N9",
                    "Q1",
                    "Q2",
                    "Q3",
                    "Q9"
            ));


}
