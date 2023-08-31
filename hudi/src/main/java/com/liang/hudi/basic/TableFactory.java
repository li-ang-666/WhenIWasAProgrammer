package com.liang.hudi.basic;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@UtilityClass
@Slf4j
public class TableFactory {
    public static String fromFile(String fileName) {
        try {
            InputStream stream = TableFactory.class.getClassLoader()
                    .getResourceAsStream("sqls/" + fileName);
            assert stream != null;
            return IOUtils.toString(stream, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("create table from file error");
            throw new RuntimeException(e);
        }
    }
}
