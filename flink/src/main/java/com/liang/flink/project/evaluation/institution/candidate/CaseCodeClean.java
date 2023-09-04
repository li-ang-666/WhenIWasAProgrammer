package com.liang.flink.project.evaluation.institution.candidate;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CaseCodeClean {
    public String evaluate(String caseDode) {
        //1.如果匹配到是属于 '暂无' '-' '——' 全为数字 全为乱码 以??开头 直接反回空字符串
        if (StringUtils.isEmpty(caseDode)) {
            return "";
        }
        if (caseDode.equals("暂无") || caseDode.equals("-")
                || caseDode.equals("——") || caseDode.matches("^\\d+")
                || caseDode.matches("^%[a-zA-Z0-9%]+") || caseDode.matches("^\\?+[0-9?]*")
        ) {
            return caseDode;
        }

        //2.首先替换掉空格和中文括号，替换以)开头的数据
        String caseDodeReplace = caseDode.replaceAll("\\s+", "").replaceAll("\"", "")
                .replaceAll("\\(", "（")
                .replaceAll("\\[", "（")
                .replaceAll("【", "（")
                .replaceAll("（+", "（")
                .replaceAll("\\)", "）")
                .replaceAll("]", "）")
                .replaceAll("】", "）")
                .replaceAll("）+", "）")
                .replaceAll("^）", "（");

        //3.对当前的数据进行匹配
        //如果是标准格式：(2016)浙0206执3062号
        if (caseDodeReplace.matches("^（\\d+）[^0-9]+\\d+.*号$")) {
            return caseDodeReplace.replaceAll("(-\\d*号)$", "号");

            //如果开头正常但是结尾多余有数组：(2020)黑1081执9999号312018692
        } else if (caseDodeReplace.matches("^（\\d+）[^0-9]+\\d+.*号.*")) {
            String pattern = "^（\\d+）.*号";
            Pattern regex = Pattern.compile(pattern);
            Matcher matcher = regex.matcher(caseDodeReplace);
            if (matcher.find()) {
                return matcher.group();
            } else {
                return caseDodeReplace;
            }

            //如果开头正常但是结尾缺少号：(2020)黑1081执9999
        } else if (caseDodeReplace.matches("^（\\d+）[^0-9]+\\d+[^号]*\\d$")) {
            return caseDodeReplace + "号";

            //如果开头为年份没有括号的：2008黄执字第02992号
        } else if (caseDodeReplace.matches("^\\d+[^0-9]+\\d+.*号$")) {
            String pattern = "^(\\d+)([^0-9]+.*号)$";
            String replacement = "（$1）$2";
            return Pattern.compile(pattern).matcher(caseDodeReplace).replaceAll(replacement);
            //如果开头为年份没有括号的：2022鲁0811民初3383
        } else if (caseDodeReplace.matches("^\\d{4}[^0-9]+\\d+[^号]*\\d$")) {
            String pattern = "^(\\d{4})([^0-9]+\\d+[^号]*)";
            String replacement = "（$1）$2号";
            return Pattern.compile(pattern).matcher(caseDodeReplace).replaceAll(replacement);
            //如果以()开头没有年份的：()鲁0613莱字第00896号
        } else if (caseDodeReplace.matches("^（）.*号$")) {
            return caseDodeReplace.replaceAll("（）", "");
        } else {
            return caseDode;
        }
    }
}
