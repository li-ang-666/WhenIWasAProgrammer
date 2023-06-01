package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.service.impl.CompanyBranchService;
import com.liang.spark.service.impl.RestrictConsumptionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Iterator;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        spark.sql("use ods");
        new CompanyBranchService().run(spark);
        new RestrictConsumptionService().run(spark);
        spark.close();
    }

    @Slf4j
    public static class HbaseSink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private final RowMapper rowMapper;
        private final Boolean isHistory;
        private HbaseTemplate hbaseTemplate;

        public HbaseSink(Config config, Boolean isHistory, RowMapper rowMapper) {
            this.config = config;
            this.rowMapper = rowMapper;
            this.isHistory = isHistory;
        }

        private void open() {
            ConfigUtils.setConfig(config);
            hbaseTemplate = new HbaseTemplate("test");
        }

        @Override
        public void call(Iterator<Row> iter) throws Exception {
            open();
            int i = 0;
            while (iter.hasNext()) {
                Row row = iter.next();
                hbaseTemplate.upsert(rowMapper.map(isHistory, row));
                i++;
            }
            log.info("hbase 写入 {} row", i);
        }
    }

    @FunctionalInterface
    public interface RowMapper extends Serializable {
        HbaseOneRow map(Boolean isHistory, Row row);
    }
}
