package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.project.ratio.path.company.RatioPathCompanyService;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class RatioPathCompanyRepair {
    public static void main(String[] args) {
        Config config = ConfigUtils.initConfig(null);
        ConfigUtils.setConfig(config);
        Set<Long> companyIds = new HashSet<>(Arrays.asList(3105209346L, 2959436664L, 3448197051L, 1215320250L, 4331793706L, 2959612408L, 4060014709L, 3048954402L, 6316867100L, 6316867100L, 6316867100L, 6316867100L, 6316867100L, 6316867100L, 6316867100L, 4374850706L, 2318861145L, 2352905739L, 2841253332L, 4337966126L, 4337966126L, 3430624228L, 5877440106L));
        new RatioPathCompanyService().invoke(companyIds);
        ConfigUtils.unloadAll();
    }
}
