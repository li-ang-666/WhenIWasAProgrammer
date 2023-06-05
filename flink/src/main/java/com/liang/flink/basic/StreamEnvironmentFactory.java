package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

@Slf4j
public class StreamEnvironmentFactory {
    private final static long CHECKPOINT_INTERVAL = 1000 * 60 * 3;
    private final static long CHECKPOINT_TIMEOUT = 1000 * 60 * 10;

    private StreamEnvironmentFactory() {
    }

    public static StreamExecutionEnvironment create(String[] args) throws Exception {
        initConfig(args);
        return initEnv();
    }

    private static void initConfig(String[] args) throws Exception {
        InputStream resourceStream;
        if (args.length == 0) {
            throw new RuntimeException("main(args) 没有传递 config 文件, program exit ...");
        } else {
            log.info("main(args) 传递 config 文件: {}", args[0]);
            try {
                resourceStream = Files.newInputStream(Paths.get(args[0]));
            } catch (java.nio.file.NoSuchFileException e) {
                resourceStream = StreamEnvironmentFactory.class.getClassLoader().getResourceAsStream(args[0]);
            }
        }
        Config config = YamlUtils.parse(resourceStream, Config.class);
        ConfigUtils.setConfig(config);
    }

    private static StreamExecutionEnvironment initEnv() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //本地checkpoint调试
        if (env instanceof LocalStreamEnvironment) {
            Configuration configuration = new Configuration();
            configuration.setString("state.checkpoints.dir", "file://" +
                    "/Users/liang/Desktop/flink-checkpoints/");
            /*configuration.setString("execution.savepoint.path", "file://" +
                    "/Users/liang/Desktop/flink-checkpoints/53b0ef2c94cda86ea614605757352069/chk-2");*/
            configuration.setString("rest.bind-port", "54321");
            env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        //获取checkpoint管理者
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //每5分钟开启一次
        checkpointConfig.setCheckpointInterval(CHECKPOINT_INTERVAL);
        //两次checkpoint之间最少间隔时间
        checkpointConfig.setMinPauseBetweenCheckpoints(CHECKPOINT_INTERVAL);
        //模式是Exactly-Once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //每次checkpoint超时是30分钟
        checkpointConfig.setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        //可以容忍的连续checkpoint次数,次数超过后任务自动停止
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //同时运行的checkpoint数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //程序停止时保留checkpoint
        checkpointConfig.enableExternalizedCheckpoints(RETAIN_ON_CANCELLATION);
        //开启非对齐的checkpoint(可跳跃的barrier)
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setForceUnalignedCheckpoints(true);
        return env;
    }
}
