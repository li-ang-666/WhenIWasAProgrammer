package com.liang.flink.project.evaluation.institution.candidate;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CaseCodeType {

    public String evaluate(String caseDode) {
        String regex = "^（\\d+）";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(caseDode);
        if (matcher.find() && matcher.group(0).replaceAll("（", "").replaceAll("）", "").length() == 4 && Integer.parseInt(matcher.group(0).replaceAll("（", "").replaceAll("）", "")) >= 2016) {
            return caseTypeMap(caseDode);
        } else {
            if (caseDode.contains("行")) {
                return "行政";
            } else if (caseDode.contains("法") || caseDode.contains("减")
                    || caseDode.contains("假") || caseDode.contains("刑")
            ) {
                return "刑事";
            } else {
                return caseTypeMap(caseDode);
            }
        }
    }

    private String caseTypeMap(String caseDode) {
        if (caseDode.contains("刑")) {
            return "刑事";
        } else if (caseDode.contains("行") && !caseDode.contains("行保")) {
            return "行政";
        } else if (caseDode.contains("赔辖")) {
            return "行政";
        } else if (caseDode.contains("民辖终")) {
            return "民事管辖上诉案件";
        } else if (caseDode.contains("民辖监")) {
            return "民事管辖监督案件";
        } else if (caseDode.contains("民辖")) {
            return "民事管辖案件";
        } else if (caseDode.contains("民初")) {
            return "民事一审案件";
        } else if (caseDode.contains("民终")) {
            return "民事二审案件";
        } else if (caseDode.contains("民监")) {
            return "民事依职权再审审查案件";
        } else if (caseDode.contains("民申")) {
            return "民事申请再审审查案件";
        } else if (caseDode.contains("民抗")) {
            return "民事抗诉再审审查案件";
        } else if (caseDode.contains("民再")) {
            return "民事再审案件";
        } else if (caseDode.contains("民撤")) {
            return "第三人撤销之诉案件";
        } else if (caseDode.contains("民特")) {
            return "特别程序案件";
        } else if (caseDode.contains("民催")) {
            return "催告案件";
        } else if (caseDode.contains("民督监")) {
            return "支付令监督案件";
        } else if (caseDode.contains("民督")) {
            return "申请支付令审查案件";
        } else if (caseDode.contains("民破")) {
            return "破产案件";
        } else if (caseDode.contains("民算")) {
            return "强制清算案件";
        } else if (caseDode.contains("民他")) {
            return "其他民事案件";
        } else if (caseDode.contains("清申")) {
            return "强制清算申请审查案件";
        } else if (caseDode.contains("破申")) {
            return "破产申请审查案件";
        } else if (caseDode.contains("清终")) {
            return "强制清算上诉案件";
        } else if (caseDode.contains("破终")) {
            return "破产上诉案件";
        } else if (caseDode.contains("清监")) {
            return "强制清算案件";
        } else if (caseDode.contains("破监")) {
            return "破产监督案件";
        } else if (caseDode.contains("强清")) {
            return "强制清算案件";
        } else if (caseDode.contains("破")) {
            return "破产案件";
        } else if (caseDode.contains("法赔")) {
            return "国家赔偿";
        } else if (caseDode.contains("委赔")) {
            return "国家赔偿";
        } else if (caseDode.contains("委赔监")) {
            return "国家赔偿";
        } else if (caseDode.contains("委赔提")) {
            return "国家赔偿";
        } else if (caseDode.contains("委赔再")) {
            return "国家赔偿";
        } else if (caseDode.contains("赔他")) {
            return "国家赔偿";
        } else if (caseDode.contains("司救民")) {
            return "国家赔偿";
        } else if (caseDode.contains("司救赔")) {
            return "国家赔偿";
        } else if (caseDode.contains("司救执")) {
            return "国家赔偿";
        } else if (caseDode.contains("司救访")) {
            return "国家赔偿";
        } else if (caseDode.contains("司救他")) {
            return "国家赔偿";
        } else if (caseDode.contains("认台")) {
            return "认可与执行申请审查案件";
        } else if (caseDode.contains("认港")) {
            return "认可与执行申请审查案件";
        } else if (caseDode.contains("认澳")) {
            return "认可与执行申请审查案件";
        } else if (caseDode.contains("认复")) {
            return "认可与执行审查复议案件";
        } else if (caseDode.contains("认他")) {
            return "认可与执行审查其他案件";
        } else if (caseDode.contains("请台送")) {
            return "请求台湾地区送达文书审查案件";
        } else if (caseDode.contains("请港送")) {
            return "请求香港特区法院送达文书审查案件";
        } else if (caseDode.contains("请澳送")) {
            return "请求澳门特区法院送达文书审查案件";
        } else if (caseDode.contains("台请送")) {
            return "送达文书案件";
        } else if (caseDode.contains("港请送")) {
            return "送达文书案件";
        } else if (caseDode.contains("澳请送")) {
            return "送达文书案件";
        } else if (caseDode.contains("请台调")) {
            return "请求台湾地区调查取证审查案件";
        } else if (caseDode.contains("请港调")) {
            return "请求香港特区法院调查取证审查案件";
        } else if (caseDode.contains("请澳调")) {
            return "请求澳门特区法院调查取证审查案件";
        } else if (caseDode.contains("台请调")) {
            return "调查取证案件";
        } else if (caseDode.contains("港请调")) {
            return "调查取证案件";
        } else if (caseDode.contains("澳请调")) {
            return "调查取证案件";
        } else if (caseDode.contains("请移管")) {
            return "接收在台湾地区被判刑人案件";
        } else if (caseDode.contains("助移管")) {
            return "向台湾地区移管被判刑人案件";
        } else if (caseDode.contains("请移赃")) {
            return "接收台湾地区移交罪赃案件";
        } else if (caseDode.contains("助移赃")) {
            return "向台湾地区移交罪赃案件";
        } else if (caseDode.contains("协外认")) {
            return "承认与执行申请审查案件";
        } else if (caseDode.contains("协他")) {
            return "承认与执行审查其他案件";
        } else if (caseDode.contains("协外送")) {
            return "送达文书案件";
        } else if (caseDode.contains("请外送")) {
            return "请求外国法院送达文书审查案件";
        } else if (caseDode.contains("协外调")) {
            return "调查取证案件";
        } else if (caseDode.contains("请外调")) {
            return "请求外国法院调查取证审查案件";
        } else if (caseDode.contains("请外移")) {
            return "接收在外国被判刑人案件";
        } else if (caseDode.contains("协外移")) {
            return "向外国移管被判刑人案件";
        } else if (caseDode.contains("请外引")) {
            return "请求外国引渡案件";
        } else if (caseDode.contains("协外引")) {
            return "协助外国引渡案件";
        } else if (caseDode.contains("司惩复")) {
            return "司法制裁复议案件";
        } else if (caseDode.contains("司惩")) {
            return "司法制裁审查案件";
        } else if (caseDode.contains("财保")) {
            return "非诉财产保全审查案件";
        } else if (caseDode.contains("行保")) {
            return "非诉财产保全审查案件";
        } else if (caseDode.contains("证保")) {
            return "非诉财产保全审查案件";
        } else if (caseDode.contains("执恢")) {
            return "恢复执行案件";
        } else if (caseDode.contains("执保")) {
            return "财产保全执行案件";
        } else if (caseDode.contains("执异")) {
            return "执行异议案件";
        } else if (caseDode.contains("执复")) {
            return "执行复议案件";
        } else if (caseDode.contains("执监")) {
            return "执行监督案件";
        } else if (caseDode.contains("执协")) {
            return "执行协调案件";
        } else if (caseDode.contains("执他")) {
            return "其他执行案件";
        } else if (caseDode.contains("执")) {
            return "首次执行案件";
        } else {
            return "";
        }
    }
}
