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
                6145631249L, 4002271280L, 4536444204L, 3406860535L, 3388728012L, 3394281421L, 6087765423L, 5534712545L, 5568883908L
        ));
        new RatioPathCompanyService().invoke(companyIds);
        ConfigUtils.unloadAll();
    }
}
