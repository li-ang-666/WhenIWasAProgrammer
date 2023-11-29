package com.liang.hudi.job;

import com.liang.hudi.basic.TableEnvironmentFactory;
import com.liang.hudi.basic.TableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.WriteOperationType;

import static org.apache.hudi.common.model.WriteOperationType.BULK_INSERT;
import static org.apache.hudi.common.model.WriteOperationType.UPSERT;

@Slf4j
public class HudiJob {
    public static void main(String[] args) {
        // create env
        StreamTableEnvironment tEnv = TableEnvironmentFactory.create();
        Configuration configuration = tEnv.getConfig().getConfiguration();
        WriteOperationType writeOperationType = WriteOperationType.valueOf(args[0]);
        if (writeOperationType == BULK_INSERT) {
            configuration.setString("parallelism.default", "16");
        } else if (writeOperationType == UPSERT) {
            configuration.setString("parallelism.default", "1");
        }
        // exec sql
        String sqls = TableFactory.fromTemplate(writeOperationType, args[1], args[2]);
        for (String sql : sqls.split(";")) {
            if (StringUtils.isNotBlank(sql)) {
                log.info("sql: {}", sql);
                tEnv.executeSql(sql);
            }
        }
    }
}
