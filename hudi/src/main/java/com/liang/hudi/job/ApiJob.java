package com.liang.hudi.job;

import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.*;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.util.HoodiePipeline;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.configuration.FlinkOptions.*;

@SuppressWarnings("unchecked")
@Slf4j
public class ApiJob {
    private static final String OBS_PATH = "obs://hadoop-obs/hudi_ods/ratio_path_company001";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //configuration.setString("rest.bind-port", "54321");
        //configuration.setString("state.checkpoints.dir", "file:///Users/liang/Desktop/flink-checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.enableCheckpointing(1000 * 60, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        // kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092")
                .setTopics("e1d4c.json.prism_shareholder_path.ratio_path_company")
                .setGroupId("hudi-demo-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> sourceStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");
        SingleOutputStreamOperator<RowData> dataStream = sourceStream.flatMap(new FlatMapFunction<String, RowData>() {
            @Override
            public void flatMap(String value, Collector<RowData> out) {
                Map<String, Object> binlogMap = JsonUtils.parseJsonObj(value);
                String type = String.valueOf(binlogMap.get("type"));
                RowKind rowKind;
                switch (type) {
                    case "INSERT":
                        rowKind = RowKind.INSERT;
                        break;
                    case "UPDATE":
                        rowKind = RowKind.UPDATE_AFTER;
                        break;
                    case "DELETE":
                        rowKind = RowKind.DELETE;
                        break;
                    default:
                        return;
                }
                long executeTime = Long.parseLong(String.valueOf(binlogMap.get("es")));
                List<Map<String, Object>> data = (List<Map<String, Object>>) binlogMap.get("data");
                for (Map<String, Object> columnMap : data) {
                    GenericRowData rowData = new GenericRowData(rowKind, 15);
                    rowData.setField(0, DecimalData.fromBigDecimal(new BigDecimal(String.valueOf(columnMap.get("id"))), 20, 0));
                    rowData.setField(1, Long.parseLong(String.valueOf(columnMap.get("company_id"))));
                    rowData.setField(2, StringData.fromString(String.valueOf(columnMap.get("shareholder_id"))));
                    rowData.setField(3, Short.parseShort(String.valueOf(columnMap.get("shareholder_entity_type"))));
                    rowData.setField(4, Long.parseLong(String.valueOf(columnMap.get("shareholder_name_id"))));
                    rowData.setField(5, DecimalData.fromBigDecimal(new BigDecimal(String.valueOf(columnMap.get("investment_ratio_total"))), 24, 12));
                    rowData.setField(6, Short.parseShort(String.valueOf(columnMap.get("is_controller"))));
                    rowData.setField(7, Short.parseShort(String.valueOf(columnMap.get("is_ultimate"))));
                    rowData.setField(8, Short.parseShort(String.valueOf(columnMap.get("is_big_shareholder"))));
                    rowData.setField(9, Short.parseShort(String.valueOf(columnMap.get("is_controlling_shareholder"))));
                    rowData.setField(10, StringData.fromString(String.valueOf(columnMap.get("equity_holding_path"))));
                    rowData.setField(11, TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(String.valueOf(columnMap.get("create_time")), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    ));
                    rowData.setField(12, TimestampData.fromLocalDateTime(
                            LocalDateTime.parse(String.valueOf(columnMap.get("update_time")), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    ));
                    rowData.setField(13, Short.parseShort(String.valueOf(columnMap.get("is_deleted"))));
                    rowData.setField(14, TimestampData.fromLocalDateTime(
                            LocalDateTime.ofEpochSecond(executeTime / 1000, 0, ZoneOffset.of("+8"))
                    ));
                    out.collect(rowData);
                }
            }
        });
        HoodiePipeline.builder("ratio_path_company")
                .column("id                         DECIMAL(20, 0)")
                .column("company_id                 BIGINT")
                .column("shareholder_id             STRING")
                .column("shareholder_entity_type    SMALLINT")
                .column("shareholder_name_id        BIGINT")
                .column("investment_ratio_total     DECIMAL(24, 12)")
                .column("is_controller              SMALLINT")
                .column("is_ultimate                SMALLINT")
                .column("is_big_shareholder         SMALLINT")
                .column("is_controlling_shareholder SMALLINT")
                .column("equity_holding_path        STRING")
                .column("create_time                TIMESTAMP(3)")
                .column("update_time                TIMESTAMP(3)")
                .column("is_deleted                 SMALLINT")
                .column("op_ts                      TIMESTAMP(3)")
                .pk("id")
                // common
                .option(PATH, OBS_PATH)
                .option(TABLE_TYPE, HoodieTableType.MERGE_ON_READ)
                // cdc
                .option(CHANGELOG_ENABLED, true)
                // index
                .option(INDEX_TYPE, HoodieIndex.IndexType.BUCKET)
                .option(BUCKET_INDEX_NUM_BUCKETS, 128)
                // write
                .option(WRITE_TASKS, 4)
                .option(WRITE_TASK_MAX_SIZE, 512)
                .option(WRITE_BATCH_SIZE, 8)
                .option(WRITE_LOG_BLOCK_SIZE, 64)
                .option(PRE_COMBINE, true)
                .option(PRECOMBINE_FIELD, "op_ts")
                // compaction
                .option(COMPACTION_SCHEDULE_ENABLED, true)
                .option(COMPACTION_ASYNC_ENABLED, false)
                .option(COMPACTION_DELTA_COMMITS, 30)
                // clean
                .option(CLEAN_ASYNC_ENABLED, true)
                .option(CLEAN_RETAIN_COMMITS, 2880)
                // sink
                .sink(dataStream, false);
        env.execute("api-job");
    }
}