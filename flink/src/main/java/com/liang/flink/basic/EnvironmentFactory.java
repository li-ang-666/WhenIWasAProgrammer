package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.StackUtils;
import com.liang.flink.service.LocalConfigFile;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

@Slf4j
@UtilityClass
public class EnvironmentFactory {
    private final static long CHECKPOINT_INTERVAL_LOCAL_TEST = 1000 * 60;
    private final static long CHECKPOINT_INTERVAL = 1000 * 60 * 3;
    private final static long CHECKPOINT_TIMEOUT = 1000 * 60 * 30;

    @SneakyThrows(ClassNotFoundException.class)
    public static StreamExecutionEnvironment create(String[] args) {
        String file;
        if (args != null && args.length > 0) {
            file = args[0];
        } else {
            String jobClassName = StackUtils.getMainFrame().getClassName();
            Class<?> jobClass = Class.forName(jobClassName);
            if (jobClass.isAnnotationPresent(LocalConfigFile.class)) {
                file = jobClass.getAnnotation(LocalConfigFile.class).value();
            } else {
                file = null;
            }
        }
        initConfig(file);
        StreamExecutionEnvironment env = initEnv();
        configEnvCkp(env);
        return env;
    }

    private static void initConfig(String file) {
        Config config = ConfigUtils.createConfig(file);
        ConfigUtils.setConfig(config);
    }

    private static StreamExecutionEnvironment initEnv() {
        StreamExecutionEnvironment tempEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        return tempEnv instanceof LocalStreamEnvironment ? initLocalEnv() : initClusterEnv();
    }

    private static StreamExecutionEnvironment initLocalEnv() {
        Configuration configuration = new Configuration();
        configuration.setString("rest.bind-port", "54321");
        configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints/");
        //configuration.setString("execution.savepoint.path", "file:/Users/liang/Desktop/flink-checkpoints/5ce93e0d9a6e1b9c28ea90383ebc9635/chk-1");
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }

    private static StreamExecutionEnvironment initClusterEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    private static void configEnvCkp(StreamExecutionEnvironment env) {
        boolean isLocal = env instanceof LocalStreamEnvironment;
        // max parallel
        env.setMaxParallelism(1024);
        // 统一checkpoint管理
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 运行周期
        checkpointConfig.setCheckpointInterval(isLocal ? CHECKPOINT_INTERVAL_LOCAL_TEST : CHECKPOINT_INTERVAL);
        // 两次checkpoint之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(isLocal ? CHECKPOINT_INTERVAL_LOCAL_TEST : CHECKPOINT_INTERVAL);
        // 模式是Exactly-Once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 超时
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        // 可以容忍的连续checkpoint次数,次数超过后任务自动停止
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(24);
        // 同时运行的checkpoint数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 程序停止时保留checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        // 开启非对齐的checkpoint(可跳跃的barrier)
        checkpointConfig.enableUnalignedCheckpoints();
    }
}
