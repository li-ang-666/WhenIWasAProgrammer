package com.liang.flink.job;

import com.liang.flink.env.StreamEnvironmentFactory;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class TestJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamEnvironmentFactory.createStreamEnvironment(null);
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> stream2 = env.fromElements(1, 2, 3);
        stream1.keyBy(e -> e)
                .coGroup(stream2.keyBy(e -> e))
                .where(e -> e)
                .equalTo(e -> e)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Integer, Integer, String>() {
                    @Override
                    public void coGroup(Iterable<Integer> iterable1, Iterable<Integer> iterable2, Collector<String> collector) throws Exception {

                    }
                });
    }
}
