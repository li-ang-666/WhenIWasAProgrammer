package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;

import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;

@Slf4j
public class HudiJob {
    public static void main(String[] args) {
        WriteOperationType opt = WriteOperationType.valueOf(args[0]);
        String dbSource = args[1];
        String tbName = args[2];
        StreamTableEnvironment tEnv;
        if (opt == BULK_INSERT) {
            tEnv = TableEnvironmentFactory.create(true);
        } else {
            tEnv = TableEnvironmentFactory.create(false);
        }
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setBoolean("pipeline.operator-chaining.enabled", false);
        configuration.setString("pipeline.name", String.format("%s.%s", dbSource, tbName));
        StreamStatementSet statementSet = tEnv.createStatementSet();
        for (String sql : TableFactory.fromTemplate(opt, dbSource, tbName).split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            if (sql.toLowerCase().contains("insert into")) {
                sql += args.length > 3 ? " WHERE " + args[3] : "";
                statementSet.addInsertSql(sql);
            } else {
                tEnv.executeSql(sql);
            }
            log.info("sql: {}", sql);
        }
        statementSet.execute();
    }
}
