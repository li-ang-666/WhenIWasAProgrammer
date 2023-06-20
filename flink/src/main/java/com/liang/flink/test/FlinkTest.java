package com.liang.flink.test;

import com.obs.services.ObsClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

@Slf4j
public class FlinkTest {
    private final static String AK = "NT5EWZ4FRH54R2R2CB8G";
    private final static String SK = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String EP = "obs.cn-north-4.myhuaweicloud.com";

    public static void main(String[] args) throws Exception {
        try (ObsClient client = new ObsClient(AK, SK, EP)) {
            String bucket = "hadoop-obs";
            String object = "hive/warehouse/test.db/no_shareholder_company_info_incr/111";
            ArrayList<String> rows = new ArrayList<>();
            for (int i = 1; i <= 1000000; i++) {
                rows.add(new Random().nextInt(Integer.MAX_VALUE) + "\001" + UUID.randomUUID());
            }
            String contents = StringUtils.join(rows, '\002') + '\002';
            byte[] contentBytes = contents.getBytes(StandardCharsets.UTF_8);
            log.info("---");
            client.putObject(bucket, object, new ByteArrayInputStream(contentBytes));
            log.info("---");
        }
    }
}