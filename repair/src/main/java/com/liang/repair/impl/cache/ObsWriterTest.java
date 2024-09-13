package com.liang.repair.impl.cache;

import com.liang.common.service.storage.ObsWriter;
import com.liang.repair.service.ConfigHolder;
import com.obs.services.ObsClient;

import java.io.ByteArrayInputStream;
import java.util.UUID;

public class ObsWriterTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        ObsWriter obsWriter = new ObsWriter("");
        ObsClient client = obsWriter.getClient();
        client.putObject(
                "jindi-bigdata",
                "company_bid_parsed_info_test/content_obs_url/uuid.txt",
                new ByteArrayInputStream(UUID.randomUUID().toString().getBytes())
        );
    }
}
