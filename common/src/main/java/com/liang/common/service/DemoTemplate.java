package com.liang.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.Queue;

@Slf4j
public class DemoTemplate extends AbstractCache<String, String> {
    private final static int BUFFER_MAX_MB = 1;
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;

    public DemoTemplate() {
        super(BUFFER_MAX_MB, DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, e -> "");
    }

    @Override
    protected void updateImmediately(String key, Queue<String> values) {
    }
}
