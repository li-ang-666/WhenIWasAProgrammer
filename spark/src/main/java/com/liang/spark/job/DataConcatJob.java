package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.template.HbaseTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.YamlUtils;
import com.liang.spark.dao.DataConcatSqlHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;

@Slf4j
public class DataConcatJob {
    public static void main(String[] args) throws Exception {
        DataConcatSqlHolder sqlHolder = new DataConcatSqlHolder();
        SparkSession spark = init(args);
        Dataset<Row> mostApplicant = spark.sql(sqlHolder.restrictedConsumptionMostApplicantSql());
        //Dataset<Row> mostRelated = spark.sql(sqlHolder.restrictedConsumptionMostRelatedRestrictedSql());
        //Dataset<Row> mostRestricted = spark.sql(sqlHolder.restrictedConsumptionMostRestrictedSql());
        mostApplicant.foreachPartition(new HbaseSink(ConfigUtils.getConfig()));
        spark.close();
    }

    public static SparkSession init(String[] args) throws Exception {
        InputStream resourceStream;
        if (args.length == 0) {
            log.warn("参数没有传递外部config文件, 从内部 resource 寻找 ...");
            resourceStream = DataConcatJob.class.getClassLoader().getResourceAsStream("config.yml");
        } else {
            log.warn("外部参数传递 config 文件: {}", args[0]);
            resourceStream = Files.newInputStream(Paths.get(args[0]));
        }
        Config config = YamlUtils.parse(resourceStream, Config.class);
        ConfigUtils.setConfig(config);
        SparkSession spark = SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200")
                .enableHiveSupport()
                .getOrCreate();
        spark.sql("use ods");
        return spark;
    }

    private static class HbaseSink implements ForeachPartitionFunction<Row> {
        private final Config config;
        private HbaseTemplate hbaseTemplate;

        private HbaseSink(Config config) {
            this.config = config;
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
                String companyId = String.valueOf(row.get(0));
                String info = String.valueOf(row.get(1));
                HbaseOneRow hbaseOneRow = new HbaseOneRow("dataConcat", companyId)
                        .put("info", info);
                hbaseTemplate.upsert(hbaseOneRow);
            }
        }
    }
}
