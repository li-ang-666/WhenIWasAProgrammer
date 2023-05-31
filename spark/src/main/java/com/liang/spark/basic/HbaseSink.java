package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.function.RowMapper;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;

public class HbaseSink implements ForeachPartitionFunction<Row> {
    private final Config config;
    private final RowMapper rowMapper;
    private HbaseTemplate hbaseTemplate;


    public HbaseSink(Config config, RowMapper rowMapper) {
        this.config = config;
        this.rowMapper = rowMapper;
    }

    private void open() {
        ConfigUtils.setConfig(config);
        hbaseTemplate = new HbaseTemplate("test");
    }

    @Override
    public void call(Iterator<Row> iter) throws Exception {
        open();
        while (iter.hasNext()) {
            Row row = iter.next();
            hbaseTemplate.upsert(rowMapper.map(row));
        }
    }
}
