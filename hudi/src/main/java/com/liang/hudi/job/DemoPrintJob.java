package com.liang.hudi.job;


public class DemoPrintJob {
    public static void main(String[] args) throws Exception {
        /*Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        DruidDataSource pool = new DruidFactory().createPool("457.prism_shareholder_path");
        DruidPooledConnection connection = pool.getConnection();
        ResultSet rs = connection.prepareStatement("select * from ratio_path_company limit 1").executeQuery();
        rs.next();
        //Row row = Row.of(rs.getObject(1), rs.getObject(2), rs.getObject(3), rs.getObject(4), rs.getObject(5), rs.getObject(6), rs.getObject(7), rs.getObject(8), rs.getObject(9), rs.getObject(10), rs.getObject(11), rs.getObject(12), rs.getObject(13), rs.getObject(14));
        Row row = Row.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
        System.out.println(row);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(6000);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStreamSource<Row> stream = env.fromElements(row);
        tEnv.createTemporaryView("t", tEnv.fromDataStream(stream).as("id", "company_id", "shareholder_id", "shareholder_entity_type", "shareholder_name_id", "investment_ratio_total", "is_controller", "is_ultimate", "is_big_shareholder", "is_controlling_shareholder", "equity_holding_path", "create_time", "update_time", "is_deleted"));
        tEnv.executeSql("select * from t").print();*/
    }
}
