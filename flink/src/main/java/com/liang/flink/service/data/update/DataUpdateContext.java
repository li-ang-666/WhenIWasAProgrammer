package com.liang.flink.service.data.update;

import com.liang.common.util.TableNameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataUpdateContext<C> {
    private final Map<String, AbstractDataUpdate<C>> map = new HashMap<>();
    private final String fullPackageName;

    public DataUpdateContext(String fullPackageName) {
        this.fullPackageName = fullPackageName;
    }

    @SuppressWarnings("unchecked")
    public DataUpdateContext<C> addClass(String shortClassName) throws Exception {
        String fullClassName = fullPackageName + "." + shortClassName;
        String tableName = TableNameUtils.humpToUnderLine(shortClassName);
        this.map.put(tableName, (AbstractDataUpdate<C>) Class.forName(fullClassName).newInstance());
        log.info("加载表处理类: {} -> {}", tableName, fullClassName);
        return this;
    }

    public AbstractDataUpdate<C> getClass(String tableName) {
        AbstractDataUpdate<C> impl = map.get(tableName);
        if (impl == null) {
            log.warn("该表无处理类: {}", tableName);
        }
        return impl;
    }
}
