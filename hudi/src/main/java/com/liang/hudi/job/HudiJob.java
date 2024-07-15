package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;

import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;

@Slf4j
public class HudiJob {
    public static void main(String[] args) {
        TableEnvironment tEnv;
        Configuration configuration;
        StatementSet statementSet;
        WriteOperationType writeOperationType = WriteOperationType.valueOf(args[0]);
        if (writeOperationType == BULK_INSERT) {
            tEnv = TableEnvironmentFactory.create(true);
            configuration = tEnv.getConfig().getConfiguration();
            statementSet = tEnv.createStatementSet();
            configuration.setInteger("execution.checkpointing.interval", 1000 * 60);
            configuration.setInteger("execution.checkpointing.min-pause", 1000 * 60);
        } else {
            tEnv = TableEnvironmentFactory.create(false);
            configuration = tEnv.getConfig().getConfiguration();
            statementSet = tEnv.createStatementSet();
        }
        String dbSource = args[1];
        String tbName = args[2];
        configuration.setString("pipeline.name", String.format("%s.%s", dbSource, tbName));
        for (String sql : TableFactory.fromTemplate(writeOperationType, dbSource, tbName).split(";")) {
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
