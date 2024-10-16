package com.liang.common.service.database.holder;

public interface MultiPoolHolder<POOL> {
    POOL getPool(String name);

    void closeAll();
}
