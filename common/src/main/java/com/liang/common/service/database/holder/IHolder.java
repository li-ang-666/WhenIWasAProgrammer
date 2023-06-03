package com.liang.common.service.database.holder;

public interface IHolder<T> {
    T getPool(String name);

    void closeAll();
}
