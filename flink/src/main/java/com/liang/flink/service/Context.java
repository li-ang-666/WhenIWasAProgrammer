package com.liang.flink.service;

import com.liang.common.util.TableNameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class Context {
    private final Map<String, AbstractDataUpdate> map = new HashMap<>();
    private final String fullPackageName;

    public Context(String fullPackageName) {
        this.fullPackageName = fullPackageName;
    }

    public Context addClass(String shortClassName) throws Exception {
        String fullClassName = fullPackageName + "." + shortClassName;
        String tableName = TableNameUtils.humpToUnderLine(shortClassName);
        this.map.put(tableName, (AbstractDataUpdate) Class.forName(fullClassName).newInstance());
        log.info("加载表处理类: {} -> {}", tableName, fullClassName);
        return this;
    }

    public AbstractDataUpdate getClass(String tableName) {
        AbstractDataUpdate impl = map.get(tableName);
        if (impl == null) {
            log.warn("该表无处理类: {}", tableName);
        }
        return impl;
    }
}
