package com.liang.spark.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.flink.project.multi.node.MultiNodeService;
import com.liang.spark.basic.SparkSessionFactory;
import com.liang.spark.basic.TableFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.Iterator;
import java.util.Map;

@Slf4j
public class MultiNodeJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.createSpark(args);
        Config config = ConfigUtils.getConfig();
        TableFactory.jdbc(spark, "463.bdp_equity", "entity_controller_details")
                .persist(StorageLevel.DISK_ONLY())
                .createOrReplaceTempView("t_control");
        TableFactory.jdbc(spark, "463.bdp_equity", "entity_beneficiary_details")
                .persist(StorageLevel.DISK_ONLY())
                .createOrReplaceTempView("t_benefit");
        MultiNodeJobForeachPartitionFunc sinkFunc = new MultiNodeJobForeachPartitionFunc(config);
        spark.sql("select distinct 'control' module, company_id_controlled id from t_control")
                .union(spark.sql("select distinct 'control' module, tyc_unique_entity_id id from t_control"))
                .repartition(1200)
                .foreachPartition(sinkFunc);
        spark.sql("select distinct 'benefit' module, tyc_unique_entity_id id from t_benefit")
                .union(spark.sql("select distinct 'benefit' module, tyc_unique_entity_id_beneficiary id from t_benefit"))
                .repartition(1200)
                .foreachPartition(sinkFunc);
    }

    @RequiredArgsConstructor
    private final static class MultiNodeJobForeachPartitionFunc implements ForeachPartitionFunction<Row> {
        private final Config config;

        @Override
        public void call(Iterator<Row> iterator) {
            ConfigUtils.setConfig(config);
            MultiNodeService service = new MultiNodeService();
            JdbcTemplate sink = new JdbcTemplate("463.bdp_equity");
            sink.enableCache();
            while (iterator.hasNext()) {
                Map<String, Object> columnMap = JsonUtils.parseJsonObj(iterator.next().json());
                String id = String.valueOf(columnMap.get("id"));
                String module = String.valueOf(columnMap.get("module"));
                sink.update(service.invoke(new com.liang.flink.job.MultiNodeJob.Input(module, id, "")));
            }
            sink.flush();
        }
    }
}
