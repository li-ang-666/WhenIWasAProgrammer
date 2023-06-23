package com.liang.common.service;

import java.util.List;

public class Template extends ListCache<String> {
    private final static int CACHE_Milliseconds = 1000;

    protected Template() {
        super(1, 1);
    }

    @Override
    protected void updateImmediately(List<String> strings) {
    }
}
