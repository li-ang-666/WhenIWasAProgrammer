package com.liang.repair.impl;

import com.alibaba.druid.pool.DruidDataSource;
import com.liang.common.service.database.factory.DruidFactory;
import com.liang.repair.trait.Runner;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Test2 implements Runner {
    @Override
    public void run(String[] args) throws Exception {
        DataSource dataSource1 = new DruidFactory().createPool("companyBase");
        DataSource dataSource2 = new DruidFactory().createPool("companyBase");

        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("remarks", "true");
        // SqlParserImpl can analysis sql dialect for sql parse
        info.setProperty("parserFactory", "org.apache.calcite.sql.parser.impl.SqlParserImpl#FACTORY");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection.getProperties());
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        Schema schema = JdbcSchema.create(rootSchema, "test", dataSource1, null, "test");
        rootSchema.add("test", schema);
        Schema schema2 = JdbcSchema.create(rootSchema, "test2", dataSource2, null, "test2");
        rootSchema.add("test2", schema2);

        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1 from test.company_index t1 " +
                " join test2.company_index t2 on t1.company_id = t2.company_id where t1.company_id = 1");
        while (resultSet.next()) {
            System.out.println(resultSet.getObject(1) + "__" + resultSet.getObject(2));
        }
    }
}
