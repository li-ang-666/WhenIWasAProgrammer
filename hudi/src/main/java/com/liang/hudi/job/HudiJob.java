package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;

@Slf4j
public class HudiJob {
    public static void main(String[] args) {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        // exec sql
        StreamStatementSet statementSet = tEnv.createStatementSet();
        for (String sql : TableFactory.fromTemplate(WriteOperationType.valueOf(args[0]), args[1], args[2]).split(";")) {
            if (StringUtils.isBlank(sql)) continue;
            log.info("sql: {}", sql);
            if (sql.toLowerCase().contains("insert into")) {
                String where = args.length > 3 ? args[3] : "id > 0";
                statementSet.addInsertSql(sql + " WHERE " + where);
            } else {
                tEnv.executeSql(sql);
            }
        }
        statementSet.execute();
    }
}
