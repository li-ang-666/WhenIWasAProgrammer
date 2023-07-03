package com.liang.repair.test;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        //companyBase: { host: "ee59dd05fc0f4bb9a2497c8d9146a53cin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com",database: "company_base",user: "jdhw_d_data_dml",password: "2s0^tFa4SLrp72" }
        // check driver exist
        Class.forName("org.apache.calcite.jdbc.Driver");
        Class.forName("com.mysql.jdbc.Driver");

        // the properties for calcite connection
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("remarks", "true");
        // SqlParserImpl can analysis sql dialect for sql parse
        info.setProperty("parserFactory", "org.apache.calcite.sql.parser.impl.SqlParserImpl#FACTORY");

        // create calcite connection and schema
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection.getProperties());
        SchemaPlus rootSchema = calciteConnection.getRootSchema();

        // code for mysql datasource
        MysqlDataSource dataSource = new MysqlDataSource();
        // please change host and port maybe like "jdbc:mysql://127.0.0.1:3306/test"
        dataSource.setUrl("jdbc:mysql://ee59dd05fc0f4bb9a2497c8d9146a53cin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/company_base");
        dataSource.setUser("jdhw_d_data_dml");
        dataSource.setPassword("2s0^tFa4SLrp72");
        // mysql schema, the sub schema for rootSchema, "test" is a schema in mysql
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource, null, "test");
        rootSchema.add("test", schema);


        // code for mysql datasource2
        MysqlDataSource dataSource2 = new MysqlDataSource();
        dataSource2.setUrl("jdbc:mysql://ee59dd05fc0f4bb9a2497c8d9146a53cin01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/company_base");
        dataSource2.setUser("jdhw_d_data_dml");
        dataSource2.setPassword("2s0^tFa4SLrp72");
        // mysql schema, the sub schema for rootSchema, "test2" is a schema in mysql
        Schema schema2 = JdbcSchema.create(rootSchema, "test2", dataSource2, null, "test2");
        rootSchema.add("test2", schema2);




        // run sql query
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1 from test.company_index t1 " +
                " join test2.company_index t2 on t1.company_id = t2.company_id where t1.company_id = 1");
        while (resultSet.next()) {
            System.out.println(resultSet.getObject(1) + "__" + resultSet.getObject(2));
        }

        statement.close();
        connection.close();
    }
}
