package com.liang.common.dto;

public enum ExecMode {
    EXEC("exec"), TEST("test");

    private final String name;

    ExecMode(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
