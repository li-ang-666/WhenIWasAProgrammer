package com.liang.repair.impl;

import com.liang.repair.service.ConfigHolder;
import com.obs.services.ObsClient;
import com.obs.services.model.ObsObject;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class ReadObs extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        String endPoint = "obs.cn-north-4.myhuaweicloud.com";
        String ak = "NT5EWZ4FRH54R2R2CB8G";
        String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
        // 创建ObsClient实例
        final ObsClient obsClient = new ObsClient(ak, sk, endPoint);
        ObsObject obsObject = obsClient.getObject("jindi-oss-wangsu", "pan_zhixing/xunjiapinggu_result_content/d0c257d2b0f0404191be1996229d3375");
        // 读取对象内容
        InputStream input = obsObject.getObjectContent();
        String content = IOUtils.toString(input, StandardCharsets.UTF_8);
        System.out.println(content);
        System.out.println(content.replaceAll("\\s", "").replaceAll(".*委托(.*?)进行评估.*", "$1"));
    }
}
