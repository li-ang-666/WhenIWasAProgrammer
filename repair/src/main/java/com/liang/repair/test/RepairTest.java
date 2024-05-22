package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        // DBConfig dbConfig = ConfigUtils.getConfig().getDbConfigs().get("430.graph_data");
        // String url = String.format("jdbc:mysql://%s:3306/%s", dbConfig.getHost(), dbConfig.getDatabase());
        // Connection connection = DriverManager.getConnection(url, dbConfig.getUser(), dbConfig.getPassword());
        // Statement statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        // statement.setFetchSize(Integer.MIN_VALUE);
        // ResultSet resultSet = statement.executeQuery("select id from company_equity_relation_details where reference_pt_year = 2024");
        // int i = 0;
        // while (resultSet.next()) {
        //     i++;
        //     if (i % 10000 == 0) {
        //         System.out.println(resultSet.getString(1));
        //     }
        // }
        try {

        } catch (Exception ignore) {

        } finally {
            int i = 1 / 0;
        }
    }
}
