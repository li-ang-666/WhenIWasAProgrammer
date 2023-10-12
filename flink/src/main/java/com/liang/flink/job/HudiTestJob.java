package com.liang.flink.job;

public class HudiTestJob {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
//        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//
//        tEnv.executeSql(ApolloUtils.get("flink-sqls"));
//        DataStream<Row> changelogStream = tEnv.toChangelogStream(tEnv.from("ods_ratio_path_company"));
//        changelogStream.map(new MapFunction<Row, RowData>() {
//            @Override
//            public RowData map(Row row) throws Exception {
//                GenericRowData rowData = new GenericRowData(row.getKind(), row.getArity());
//                for (int i = 0; i < row.getArity(); i++) {
//                    rowData.setField(i, row.getField(i));
//                }
//                return rowData;
//            }
//        }).print();
//        env.execute();
    }
}
