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
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        Configuration configuration = tEnv.getConfig().getConfiguration();
        // exec sql
        StreamStatementSet statementSet = tEnv.createStatementSet();
        WriteOperationType writeOperationType = WriteOperationType.valueOf(args[0]);
        String dbSource = args[1];
        String tbName = args[2];
        configuration.setString("pipeline.name", String.format("%s.%s", dbSource, tbName));
        if (writeOperationType == BULK_INSERT) {
            configuration.setString("execution.runtime-mode", "BATCH");
            configuration.setInteger("execution.checkpointing.interval", 1000 * 60);
            configuration.setInteger("execution.checkpointing.min-pause", 1000 * 60);
        }
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
