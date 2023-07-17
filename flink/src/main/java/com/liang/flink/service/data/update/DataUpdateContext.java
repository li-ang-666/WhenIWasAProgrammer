package com.liang.flink.service.data.update;

import com.liang.common.util.TableNameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataUpdateContext<OUT> {
    private final Map<String, IDataUpdate<OUT>> map = new HashMap<>();
    private final String projectName;

    public DataUpdateContext(String projectName) {
        this.projectName = projectName;
    }

    @SuppressWarnings("unchecked")
    public DataUpdateContext<OUT> addImpl(String implName) throws Exception {
        String fullClassName = String.format("com.liang.flink.project.%s.impl.%s", projectName, implName);
        String tableName = TableNameUtils.humpToUnderLine(implName);
        map.put(tableName, (IDataUpdate<OUT>) Class.forName(fullClassName).newInstance());
        log.info("加载表处理类: {} -> {}", tableName, fullClassName);
        return this;
    }

    // 同 package 可见
    IDataUpdate<OUT> getClass(String tableName) {
        return map.get(tableName);
    }
}
