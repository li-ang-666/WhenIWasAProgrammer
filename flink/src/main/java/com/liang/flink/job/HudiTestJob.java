package com.liang.flink.job;

import com.liang.common.util.ApolloUtils;
import com.liang.flink.basic.EnvironmentFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class HudiTestJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(ApolloUtils.get("flink-sqls"));
        DataStream<Row> changelogStream = tEnv.toChangelogStream(tEnv.from("ods_ratio_path_company"));
        changelogStream.map(new MapFunction<Row, RowData>() {
            @Override
            public RowData map(Row row) throws Exception {
                GenericRowData rowData = new GenericRowData(row.getKind(), row.getArity());
                for (int i = 0; i < row.getArity(); i++) {
                    rowData.setField(i, row.getField(i));
                }
                return rowData;
            }
        }).print();
        env.execute();
    }
}
