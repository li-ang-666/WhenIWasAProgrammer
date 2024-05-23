package com.liang.repair.test;

import cn.hutool.core.collection.LineIter;
import cn.hutool.core.io.IoUtil;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.JsonUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ObsWriter obsWriter = new ObsWriter("obs://hadoop-obs/flink/group/", ObsWriter.FileFormat.JSON);
        obsWriter.enableCache();
        InputStream resourceAsStream = RepairTest.class.getClassLoader().getResourceAsStream("aaa.txt");
        LineIter iterator = IoUtil.lineIter(resourceAsStream, StandardCharsets.UTF_8);
        while (iterator.hasNext()) {
            String line = iterator.next();
            String[] split = line.split("\\s+");
            System.out.println(Arrays.toString(split));
            HashMap<String, Object> columnMap = new HashMap<String, Object>() {{
                put("company_id", split[0]);
                put("company_name", split[1]);
            }};
            obsWriter.update(JsonUtils.toString(columnMap));
        }
        obsWriter.flush();
    }
}
