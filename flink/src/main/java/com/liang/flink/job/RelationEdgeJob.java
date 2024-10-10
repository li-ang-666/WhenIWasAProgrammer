package com.liang.flink.job;

import cn.hutool.core.util.StrUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.liang.common.dto.Config;
import com.liang.common.dto.config.FlinkConfig;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
beeline

!sh hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/edge/*

drop table if exists test.relation_edge;
create external table if not exists test.relation_edge(
  `row` string
)stored as textfile location 'obs://hadoop-obs/flink/relation/edge';

*/

// spark-sql

// select count(1) from test.relation_edge;

// insert overwrite table test.relation_edge select /*+ REPARTITION(7) */ * from test.relation_edge;
@Slf4j
@LocalConfigFile("relation-edge.yml")
public class RelationEdgeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        KeyedStream<Row, String> stream = StreamFactory.create(env)
                .rebalance()
                .flatMap(new RelationEdgeMapper(config))
                .name("RelationEdgeMapper")
                .uid("RelationEdgeMapper")
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .keyBy(new RelationEdgeKeySelector());
        FlinkConfig.SourceType sourceType = config.getFlinkConfig().getSourceType();
        if (sourceType == FlinkConfig.SourceType.Repair) {
            stream
                    .addSink(new RelationEdgeObsSink(config))
                    .name("RelationEdgeObsSink")
                    .uid("RelationEdgeObsSink")
                    .setParallelism(config.getFlinkConfig().getOtherParallel());
        } else {
            stream
                    .addSink(new RelationEdgeKafkaSink(config))
                    .name("RelationEdgeKafkaSink")
                    .uid("RelationEdgeKafkaSink")
                    .setParallelism(30);
        }
        env.execute("RelationEdgeJob");
    }

    private enum Relation {
        LEGAL, HIS_LEGAL, AC, HIS_INVEST, INVEST, BRANCH
    }

    private static final class RelationEdgeKeySelector implements KeySelector<Row, String> {
        @Override
        public String getKey(Row row) {
            return String.join("-", row.getSourceId(), row.getRelation().toString(), row.getTargetId());
        }
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeMapper extends RichFlatMapFunction<SingleCanalBinlog, Row> {
        private final Config config;
        private final RelationEdgeKeySelector relationEdgeKeySelector = new RelationEdgeKeySelector();
        private final Map<String, String> dictionary = new HashMap<>();
        private JdbcTemplate prismBoss157;
        private JdbcTemplate companyBase435;
        private JdbcTemplate bdpEquity463;
        private JdbcTemplate bdpPersonnel466;
        private JdbcTemplate graphData430;

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            dictionary.put("1", "法定代表人");
            dictionary.put("2", "负责人");
            dictionary.put("3", "经营者");
            dictionary.put("4", "投资人");
            dictionary.put("5", "执行事务合伙人");
            dictionary.put("6", "法定代表人|负责人");
            prismBoss157 = new JdbcTemplate("157.prism_boss");
            companyBase435 = new JdbcTemplate("435.company_base");
            bdpEquity463 = new JdbcTemplate("463.bdp_equity");
            bdpPersonnel466 = new JdbcTemplate("466.bdp_personnel");
            graphData430 = new JdbcTemplate("430.graph_data");
        }

        @Override
        public void flatMap(SingleCanalBinlog singleCanalBinlog, Collector<Row> out) {
            ArrayList<Row> results = new ArrayList<>();
            String table = singleCanalBinlog.getTable();
            switch (table) {
                case "company_legal_person":
                    // 法人
                    parseLegalPerson(singleCanalBinlog, results);
                    break;
                case "entity_controller_details_new":
                    // 实控人
                    parseController(singleCanalBinlog, results);
                    break;
                case "company_equity_relation_details":
                    // 股东
                    parseShareholder(singleCanalBinlog, results);
                    break;
                case "company_branch":
                    // 分支机构
                    parseBranch(singleCanalBinlog, results);
                    break;
                case "entity_investment_history_fusion_details":
                    // 历史股东
                    parseHisShareholder(singleCanalBinlog, results);
                    break;
                case "entity_legal_rep_list_total":
                    // 历史法人
                    parseHisLegalPerson(singleCanalBinlog, results);
                    break;
                default:
                    log.error("wrong table: {}", table);
                    break;
            }
            results.forEach(row -> {
                if (row.isValid()) {
                    String key = relationEdgeKeySelector.getKey(row);
                    long currentId = Long.parseLong(row.getId());
                    Long maxId = kv.queryForObject("SELECT v FROM relation_kv WHERE k = " + SqlUtils.formatValue(key),
                            rs -> rs.getLong(1));
                    // 同id先插后删, 所以是 大于等于
                    if (maxId == null || currentId >= maxId) {
                        kv.update(String.format("INSERT INTO relation_kv (k, v) VALUES (%s, %s) ON DUPLICATE KEY UPDATE k = VALUES(k), v = VALUES(v)",
                                SqlUtils.formatValue(key), currentId));
                        out.collect(row);
                    }
                }
            });
        }

        // 法人 -> 公司
        private void parseLegalPerson(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String pid = (String) columnMap.get("legal_rep_human_id");
                String gid = (String) columnMap.get("legal_rep_name_id");
                String legalPersonId = TycUtils.isTycUniqueEntityId(pid) ? pid : gid;
                String companyId = (String) columnMap.get("company_id");
                String identity = (String) columnMap.get("legal_rep_display_name");
                return new Row(id, legalPersonId, companyId, Relation.LEGAL, identity, null);
            };
            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 实控人 -> 公司
        private void parseController(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String shareholderId = (String) columnMap.get("tyc_unique_entity_id");
                String companyId = (String) columnMap.get("company_id_controlled");
                return new Row(id, shareholderId, companyId, Relation.AC, "", null);
            };
            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 股东 -> 公司
        private void parseShareholder(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String shareholderId = (String) columnMap.get("shareholder_id");
                String companyId = (String) columnMap.get("company_id");
                String equityRatio = (String) columnMap.get("equity_ratio");
                return new Row(id, shareholderId, companyId, Relation.INVEST, equityRatio, null);
            };
            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty())
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 分公司 -> 总公司
        private void parseBranch(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String branchCompanyId = (String) columnMap.get("branch_company_id");
                String companyId = (String) columnMap.get("company_id");
                return new Row(id, branchCompanyId, companyId, Relation.BRANCH, "", null);
            };

            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("is_deleted")))
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 历史股东 -> 公司
        private void parseHisShareholder(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String shareholderGid = (String) columnMap.get("entity_name_id");
                String shareholderType = (String) columnMap.get("entity_type_id");
                String companyId = (String) columnMap.get("company_id_invested");
                String shareholderId = "2".equals(shareholderType) ? queryPid(companyId, shareholderGid) : shareholderGid;
                String investmentRatio = StrUtil.nullToDefault((String) columnMap.get("investment_ratio"), "");
                return new Row(id, shareholderId, companyId, Relation.HIS_INVEST, investmentRatio, null);
            };

            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("delete_status")))
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        // 历史法人 -> 公司
        private void parseHisLegalPerson(SingleCanalBinlog singleCanalBinlog, List<Row> results) {
            Map<String, Object> beforeColumnMap = singleCanalBinlog.getBeforeColumnMap();
            Map<String, Object> afterColumnMap = singleCanalBinlog.getAfterColumnMap();
            Function<Map<String, Object>, Row> f = columnMap -> {
                String id = (String) columnMap.get("id");
                String shareholderId = (String) columnMap.get("tyc_unique_entity_id_legal_rep");
                String companyId = (String) columnMap.get("tyc_unique_entity_id");
                String displayId = (String) columnMap.get("legal_rep_type_display_name");
                String displayName = dictionary.getOrDefault(displayId, "法定代表人|负责人");
                return new Row(id, shareholderId, companyId, Relation.HIS_LEGAL, displayName, null);
            };

            if (!beforeColumnMap.isEmpty())
                results.add(f.apply(beforeColumnMap).setOpt(CanalEntry.EventType.DELETE));
            if (!afterColumnMap.isEmpty() && "0".equals(afterColumnMap.get("delete_status")) && "1".equals(afterColumnMap.get("is_history_legal_rep")))
                results.add(f.apply(afterColumnMap).setOpt(CanalEntry.EventType.INSERT));
        }

        private String queryPid(String companyGid, String humanGid) {
            String sql = new SQL().SELECT("human_pid")
                    .FROM("company_human_relation")
                    .WHERE("company_graph_id = " + SqlUtils.formatValue(companyGid))
                    .WHERE("human_graph_id = " + SqlUtils.formatValue(humanGid))
                    .WHERE("deleted = 0")
                    .toString();
            return prismBoss157.queryForObject(sql, rs -> rs.getString(1));
        }
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeObsSink extends RichSinkFunction<Row> implements CheckpointedFunction {
        private final Config config;
        private ObsWriter obsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            obsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/edge/", ObsWriter.FileFormat.CSV);
            obsWriter.enableCache();
        }

        @Override
        public void invoke(Row row, Context context) {
            obsWriter.update(row.toCsv());
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            obsWriter.flush();
        }

        @Override
        public void finish() {
            obsWriter.flush();
        }


        @Override
        public void close() {
            obsWriter.flush();
        }
    }

    @RequiredArgsConstructor
    private static final class RelationEdgeKafkaSink extends RichSinkFunction<Row> implements CheckpointedFunction {
        private static final String BOOTSTRAP_SERVERS = "10.99.202.90:9092,10.99.206.80:9092,10.99.199.2:9092";
        private static final String TOPIC = "rds.json.relation.edge";
        private final Config config;
        private final Lock lock = new ReentrantLock(true);
        private KafkaProducer<byte[], byte[]> producer;
        private int partition;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
            // ack
            properties.put(ProducerConfig.ACKS_CONFIG, String.valueOf(1));
            // retry
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(60 * 1000));
            properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(3));
            properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(2 * 1000));
            // in order
            properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, String.valueOf(1));
            // performance cache time
            properties.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(1000));
            // performance cache memory
            properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(16 * 1024 * 1024));
            properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(16 * 1024 * 1024));
            properties.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(1024 * 1024));
            producer = new KafkaProducer<>(properties);
            partition = getRuntimeContext().getIndexOfThisSubtask();
        }

        @Override
        public void invoke(Row row, Context context) {
            try {
                lock.lock();
                producer.send(new ProducerRecord<>(TOPIC, partition, null, row.toJson().getBytes(StandardCharsets.UTF_8)));
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            try {
                lock.lock();
                producer.flush();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void finish() {
            try {
                lock.lock();
                producer.flush();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            try {
                lock.lock();
                producer.flush();
            } finally {
                lock.unlock();
            }
        }
    }

    @Data
    @AllArgsConstructor
    @Accessors(chain = true)
    private static final class Row implements Serializable {
        private String id;
        private String sourceId;
        private String targetId;
        private Relation relation;
        private String other;
        private CanalEntry.EventType opt;

        public boolean isValid() {
            return TycUtils.isTycUniqueEntityId(sourceId) && TycUtils.isUnsignedId(targetId);
        }

        public String toCsv() {
            return Stream.of(sourceId, targetId, relation, other)
                    .map(value -> String.valueOf(value).replaceAll("[\"',\\s]", ""))
                    .collect(Collectors.joining(","));
        }

        public String toJson() {
            return JsonUtils.toString(new LinkedHashMap<String, Object>() {{
                put("id", id);
                put("source_id", sourceId);
                put("relation_type", relation);
                put("target_id", targetId);
                put("ext_info", other);
                put("is_deleted", opt == CanalEntry.EventType.DELETE ? 1 : 0);
            }});
        }
    }
}
