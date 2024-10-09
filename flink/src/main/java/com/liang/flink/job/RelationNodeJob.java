package com.liang.flink.job;

import com.liang.common.dto.Config;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Map;

/*
hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/node/company/
hdfs dfs -rm -r -f -skipTrash obs://hadoop-obs/flink/relation/node/human/


drop table if exists test.relation_node_company;
create external table if not exists test.relation_node_company(
  `row` string
)stored as textfile location 'obs://hadoop-obs/flink/relation/node/company';


drop table if exists test.relation_node_human;
create external table if not exists test.relation_node_human(
  `row` string
)stored as textfile location 'obs://hadoop-obs/flink/relation/node/human';


select count(1) from test.relation_node_company;
select count(1) from test.relation_node_human;
*/

// insert overwrite table test.relation_node_company select /*+ REPARTITION(4) */ * from test.relation_node_company;
// insert overwrite table test.relation_node_human select /*+ REPARTITION(4) */ * from test.relation_node_human;
@Slf4j
@LocalConfigFile("relation-node.yml")
public class RelationNodeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .rebalance()
                .addSink(new RelationNodeSink(config))
                .name("RelationNodeSink")
                .uid("RelationNodeSink")
                .setParallelism(config.getFlinkConfig().getOtherParallel());
        env.execute("RelationNodeJob");
    }

    @RequiredArgsConstructor
    private static final class RelationNodeSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private final Roaring64Bitmap emptyCompany = new Roaring64Bitmap();
        private ObsWriter companyObsWriter;
        private ObsWriter humanObsWriter;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            ConfigUtils.setConfig(config);
            companyObsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/node/company/", ObsWriter.FileFormat.CSV);
            companyObsWriter.enableCache();
            humanObsWriter = new ObsWriter("obs://hadoop-obs/flink/relation/node/human/", ObsWriter.FileFormat.CSV);
            humanObsWriter.enableCache();
            new JdbcTemplate("116.prism")
                    .streamQuery(true,
                            "select graph_id from entity_empty_index where entity_type = 2 and deleted = 0",
                            rs -> emptyCompany.add(rs.getLong(1)));
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            String table = singleCanalBinlog.getTable();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            if ("company_index".equals(table)) {
                long companyId = Long.parseLong((String) columnMap.get("company_id"));
                String odsStatus = (String) columnMap.get("company_registation_status");
                int status;
                if (odsStatus.contains("注销") && !odsStatus.contains("未注销")) {
                    status = 1;
                } else if (odsStatus.contains("吊销")) {
                    status = 2;
                } else {
                    status = 0;
                }
                boolean isEmpty = emptyCompany.contains(companyId);
                String companyName = ((String) columnMap.get("company_name")).replaceAll("[\"',\\s]", "");
                companyObsWriter.update(String.join(",", String.valueOf(companyId), "company", String.valueOf(status), String.valueOf(isEmpty), companyName));
            } else if ("human".equals(table)) {
                String pid = (String) columnMap.get("human_id");
                String gid = (String) columnMap.get("human_name_id");
                String name = ((String) columnMap.get("human_name")).replaceAll("[\"',\\s]", "");
                if (TycUtils.isTycUniqueEntityId(pid) && TycUtils.isUnsignedId(gid)) {
                    humanObsWriter.update(String.join(",", pid, "human", gid, name));
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            companyObsWriter.flush();
            humanObsWriter.flush();
        }

        @Override
        public void close() {
            companyObsWriter.flush();
            humanObsWriter.flush();
        }

        @Override
        public void finish() {
            companyObsWriter.flush();
            humanObsWriter.flush();
        }
    }
}
