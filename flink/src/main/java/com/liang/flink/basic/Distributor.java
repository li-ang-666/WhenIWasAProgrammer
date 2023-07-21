package com.liang.flink.basic;

import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.HashMap;

public class Distributor implements KeySelector<SingleCanalBinlog, String> {
    private final HashMap<String, Mapper> map = new HashMap<>();

    public Distributor with(String tableName, Mapper mapper) {
        map.put(tableName, mapper);
        return this;
    }

    @Override
    public String getKey(SingleCanalBinlog singleCanalBinlog) throws Exception {
        Mapper mapper = map.get(singleCanalBinlog.getTable());
        if (mapper == null) {
            return "";
        } else {
            return mapper.map(singleCanalBinlog);
        }
    }

    @FunctionalInterface
    public interface Mapper {
        String map(SingleCanalBinlog singleCanalBinlog);
    }
}
