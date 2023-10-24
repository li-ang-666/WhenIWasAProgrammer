package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RatioPathCompanyRepair {
    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        Set<Long> companyIds = new HashSet<>(Arrays.asList(
                3191323651L
        ));
        new RatioPathCompanyService().invoke(companyIds);
        ConfigUtils.unloadAll();
    }
}
