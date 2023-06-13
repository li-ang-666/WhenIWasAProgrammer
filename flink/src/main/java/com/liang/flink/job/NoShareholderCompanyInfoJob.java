package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkSource;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.basic.StreamEnvironmentFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.high.level.api.KafkaStreamFactory;
import com.liang.flink.high.level.api.RepairStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NoShareholderCompanyInfoJob {
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = new String[]{"data-concat.yml"};
        StreamExecutionEnvironment streamEnvironment = StreamEnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        DataStream<SingleCanalBinlog> stream = config.getFlinkSource() == FlinkSource.Repair ?
                RepairStreamFactory.create(streamEnvironment) :
                KafkaStreamFactory.create(streamEnvironment, 5);
        stream.rebalance();
        streamEnvironment.execute("NoShareholderCompanyInfoJob");
    }
}
