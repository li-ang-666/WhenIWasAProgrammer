package com.liang.flink.service.data.update;

import com.liang.common.util.TableNameUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DataUpdateContext<OUT> {
    private final Map<String, AbstractDataUpdate<OUT>> map = new HashMap<>();
    private final String fullPackageName;

    public DataUpdateContext(String fullPackageName) {
        this.fullPackageName = fullPackageName;
    }

    @SuppressWarnings("unchecked")
    public DataUpdateContext<OUT> addClass(String shortClassName) throws Exception {
        String fullClassName = fullPackageName + "." + shortClassName;
        String tableName = TableNameUtils.humpToUnderLine(shortClassName);
        map.put(tableName, (AbstractDataUpdate<OUT>) Class.forName(fullClassName).newInstance());
        log.info("加载表处理类: {} -> {}", tableName, fullClassName);
        return this;
    }

    public AbstractDataUpdate<OUT> getClass(String tableName) {
        return map.get(tableName);
    }
}
