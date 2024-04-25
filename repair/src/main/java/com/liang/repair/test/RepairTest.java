package com.liang.repair.test;

import com.liang.common.dto.Config;
import com.liang.common.util.ApolloUtils;
import com.liang.common.util.ConfigUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        ApolloUtils.get("queryHasController");
    }
}
