package com.liang.flink.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class FlinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = new StreamExecutionEnvironment();
        env.fromSource(new MySource(), WatermarkStrategy.noWatermarks(), "MySource")
                .print();
        env.execute();
    }

    @Slf4j
    private static class MySource implements Source<String, SourceSplit, Character> {
        @Override
        public Boundedness getBoundedness() {
            return null;
        }

        @Override
        public SplitEnumerator<SourceSplit, Character> createEnumerator(SplitEnumeratorContext<SourceSplit> enumContext) throws Exception {
            return null;
        }

        @Override
        public SplitEnumerator<SourceSplit, Character> restoreEnumerator(SplitEnumeratorContext<SourceSplit> enumContext, Character checkpoint) throws Exception {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            return null;
        }

        @Override
        public SimpleVersionedSerializer<Character> getEnumeratorCheckpointSerializer() {
            return null;
        }

        @Override
        public SourceReader<String, SourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
            return null;
        }
    }
}