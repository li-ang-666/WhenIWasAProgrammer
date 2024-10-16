package com.liang.common.service.database.holder;

public interface PoolHolder<POOL> {
    POOL getPool(String name);

    void closeAll();
}
