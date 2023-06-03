package com.liang.common.service.database.factory;

public interface IFactory<T> {
    T createPool(String name);
}
