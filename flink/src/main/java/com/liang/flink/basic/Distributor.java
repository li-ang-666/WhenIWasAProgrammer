package com.liang.flink.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.flink.dto.SingleCanalBinlog;
import org.apache.flink.api.java.functions.KeySelector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Distributor implements KeySelector<SingleCanalBinlog, String> {
    private final Config config;
    private final Map<String, SingleCanalBinlogMapper> table2Mapper = new HashMap<>();
    private boolean opened = false;

    public Distributor() {
        this.config = null;
    }

    public Distributor(Config config) {
        this.config = config;
    }

    public Distributor with(String tableName, SingleCanalBinlogMapper singleCanalBinlogMapper) {
        table2Mapper.put(tableName, singleCanalBinlogMapper);
        return this;
    }

    @Override
    public String getKey(SingleCanalBinlog singleCanalBinlog) {
        if (!opened && config != null) {
            ConfigUtils.setConfig(config);
            opened = true;
        }
        SingleCanalBinlogMapper singleCanalBinlogMapper = table2Mapper.get(singleCanalBinlog.getTable());
        if (singleCanalBinlogMapper == null) {
            return "";
        } else {
            return singleCanalBinlogMapper.map(singleCanalBinlog);
        }
    }

    @FunctionalInterface
    public interface SingleCanalBinlogMapper extends Serializable {
        String map(SingleCanalBinlog singleCanalBinlog);
    }
}
