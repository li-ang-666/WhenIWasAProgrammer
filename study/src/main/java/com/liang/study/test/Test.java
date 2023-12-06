package com.liang.study.test;

public class Test {
    public static void main(String[] args) throws Exception {
        String s = "ab、、、";
        System.out.println(s);
        System.out.println(s.replaceAll("、+", "、"));
    }
}
