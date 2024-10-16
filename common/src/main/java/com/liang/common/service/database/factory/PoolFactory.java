package com.liang.common.service.database.factory;

public interface PoolFactory<CONFIG, POOL> {
    POOL createPool(CONFIG config);

    POOL createPool(String name);
}
