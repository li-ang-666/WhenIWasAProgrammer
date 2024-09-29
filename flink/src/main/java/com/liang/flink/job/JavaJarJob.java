package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.repair.RepairSplitEnumerator;
import com.liang.flink.service.LocalConfigFile;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
@LocalConfigFile("java-jar.yml")
public class JavaJarJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        new RepairSplitEnumerator().getAllIds(config.getRepairTasks().get(0));
        env.execute("JavaJarJob");
    }
}
