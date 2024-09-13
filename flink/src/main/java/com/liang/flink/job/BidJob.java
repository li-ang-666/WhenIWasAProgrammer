package com.liang.flink.job;


import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.service.storage.ObsWriter;
import com.liang.common.util.AreaCodeUtils;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.flink.basic.EnvironmentFactory;
import com.liang.flink.basic.StreamFactory;
import com.liang.flink.dto.SingleCanalBinlog;
import com.liang.flink.service.LocalConfigFile;
import com.obs.services.ObsClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("unchecked")
@Slf4j
@LocalConfigFile("bid.yml")
public class BidJob {
    private static final List<String> COLUMNS = Arrays.asList(
            "id",
            "main_id",
            "bid_uuid",
            "bid_title",
            "bid_content",
            "bid_link",
            "bid_province",
            "bid_city",
            "public_info_lv1",
            "public_info_lv2",
            "bid_type",
            "bid_publish_time",
            "bid_item_num",
            "bid_contract_num",
            "purchaser",
            "proxy_unit",
            "bid_winner_info_json",
            "bid_winner",
            "winning_bid_amt_json",
            "winning_bid_amt_json_clean",
            "budget_amt_json",
            "budget_amt_json_clean",
            "bid_announcement_type",
            "create_time",
            "update_time",
            "is_dirty",
            "is_deleted"
    );
    private static final List<String> VOLCANIC_RDS = Arrays.asList(
            "volcanic_cloud_0",
            "volcanic_cloud_1",
            "volcanic_cloud_2",
            "volcanic_cloud_3",
            "volcanic_cloud_4",
            "volcanic_cloud_5",
            "volcanic_cloud_6",
            "volcanic_cloud_7",
            "volcanic_cloud_8",
            "volcanic_cloud_9",
            "volcanic_cloud_10"
    );
    private static final String SINK_RDS = "448.operating_info";
    private static final String SINK_TABlE = "company_bid_parsed_info";


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = EnvironmentFactory.create(args);
        Config config = ConfigUtils.getConfig();
        StreamFactory.create(env)
                .keyBy(e -> (String) e.getColumnMap().get("uuid"))
                .addSink(new BidSink(config))
                .setParallelism(config.getFlinkConfig().getOtherParallel())
                .name("BidSink")
                .uid("BidSink");
        env.execute("BidJob");
    }

    @RequiredArgsConstructor
    private static final class BidSink extends RichSinkFunction<SingleCanalBinlog> implements CheckpointedFunction {
        private final Config config;
        private JdbcTemplate sink;
        private JdbcTemplate volcanic;
        private JdbcTemplate dataBid104;
        private JdbcTemplate companyBase435;
        private ObsClient obsClient;

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void open(Configuration parameters) {
            ConfigUtils.setConfig(config);
            sink = new JdbcTemplate(SINK_RDS);
            sink.enableCache();
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            String volcanicRds = VOLCANIC_RDS.get(taskIdx % VOLCANIC_RDS.size());
            volcanic = new JdbcTemplate(volcanicRds);
            dataBid104 = new JdbcTemplate("104.data_bid");
            companyBase435 = new JdbcTemplate("435.company_base");
            obsClient = new ObsWriter("").getClient();
        }

        @Override
        public void invoke(SingleCanalBinlog singleCanalBinlog, Context context) {
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, Object> columnMap = singleCanalBinlog.getColumnMap();
            String id = (String) columnMap.get("id");
            // 删除
            String deleteSql = new SQL().DELETE_FROM(SINK_TABlE)
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .OR()
                    .WHERE("main_id = " + SqlUtils.formatValue(id))
                    .toString();
            sink.update(deleteSql);
            // 查询company_bid
            String query104Sql = new SQL().SELECT("*")
                    .FROM("company_bid")
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            List<Map<String, Object>> companyBidColumnMaps = dataBid104.queryForColumnMaps(query104Sql);
            if (companyBidColumnMaps.isEmpty()) {
                return;
            }
            Map<String, Object> companyBidColumnMap = companyBidColumnMaps.get(0);
            // 整理company_bid相关数据
            String uuid = (String) companyBidColumnMap.get("uuid");
            resultMap.put("id", id);
            resultMap.put("bid_uuid", uuid);
            resultMap.put("main_id", id);
            resultMap.put("bid_title", companyBidColumnMap.get("title"));
            resultMap.put("bid_link", companyBidColumnMap.get("link"));
            String odsPublishTime = (String) companyBidColumnMap.get("publish_time");
            resultMap.put("bid_publish_time", odsPublishTime != null && !odsPublishTime.startsWith("0000") ? odsPublishTime : null);
            resultMap.put("is_deleted", companyBidColumnMap.get("deleted"));
            resultMap.put("bid_announcement_type", StrUtil.blankToDefault((String) companyBidColumnMap.get("type"), ""));
            String content = (String) companyBidColumnMap.get("content");
            obsClient.putObject(
                    "jindi-bigdata",
                    "company_bid_parsed_info/content_obs_url/" + uuid + ".txt",
                    new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
            );
            resultMap.put("bid_content", "http://jindi-bigdata.obs.cn-north-4.myhuaweicloud.com/company_bid_parsed_info/content_obs_url/" + uuid + ".txt");
            // 查询大模型
            String queryVolcanicSql = new SQL().SELECT("*")
                    .FROM("bid_tender_details")
                    .WHERE("id = " + SqlUtils.formatValue(id))
                    .toString();
            List<Map<String, Object>> volcanicColumnMaps = volcanic.queryForColumnMaps(queryVolcanicSql);
            if (volcanicColumnMaps.isEmpty()) {
                write(resultMap);
                return;
            }
            Map<String, Object> volcanicColumnMap = volcanicColumnMaps.get(0);
            // 整理大模型相关数据
            resultMap.put("public_info_lv1", volcanicColumnMap.get("announcement_type_first"));
            resultMap.put("bid_province", StrUtil.blankToDefault(AreaCodeUtils.getCode((String) volcanicColumnMap.get("project_province")), ""));
            resultMap.put("bid_city", StrUtil.blankToDefault(AreaCodeUtils.getCode((String) volcanicColumnMap.get("project_city")), ""));
            // 招标方 or 采购方
            String purchaser = (String) volcanicColumnMap.get("tender_info");
            String parsedPurchaser = parsePurchaser(purchaser);
            if (!"[]".equals(parsedPurchaser)) {
                resultMap.put("purchaser", "[" + parsedPurchaser + "]");
            }
            // 中标方 or 供应方
            String winner = (String) volcanicColumnMap.get("winning_bid_info");
            Tuple2<String, String> parsedWinner = parseWinner(winner);
            if (!"[]".equals(parsedWinner.f0)) {
                resultMap.put("bid_winner", "[" + parsedWinner.f0 + "]");
            }
            // 中标金额
            resultMap.put("winning_bid_amt_json", winner);
            if (!"[]".equals(parsedWinner.f1)) {
                resultMap.put("winning_bid_amt_json_clean", "[" + parsedWinner.f1 + "]");
            }
            write(resultMap);
        }

        private void write(Map<String, Object> resultMap) {
            resultMap.put("is_dirty", "0");
            for (String column : COLUMNS) {
                if (!resultMap.containsKey(column)) {
                    resultMap.put(column, "");
                }
            }
            resultMap.remove("create_time");
            resultMap.remove("update_time");
            String onDuplicateKeyUpdate = SqlUtils.onDuplicateKeyUpdate(COLUMNS);
            Tuple2<String, String> insert = SqlUtils.columnMap2Insert(resultMap);
            String sql = new SQL().INSERT_INTO(SINK_TABlE)
                    .INTO_COLUMNS(insert.f0)
                    .INTO_VALUES(insert.f1)
                    .toString() + onDuplicateKeyUpdate;
            sink.update(sql);
        }

        private String parsePurchaser(String json) {
            List<Object> list = JsonUtils.parseJsonArr(json);
            List<Map<String, Object>> maps = list.stream()
                    .map(map -> (String) (((Map<String, Object>) map).get("tender_organization")))
                    .filter(this::isValid)
                    .flatMap(names -> Stream.of(names.split("、")))
                    .filter(this::isValid)
                    .map(name -> new HashMap<String, Object>() {{
                                put("gid", queryCompanyId(name));
                                put("name", name);
                            }}
                    )
                    .collect(Collectors.toList());
            return JsonUtils.toString(maps);
        }

        private Tuple2<String, String> parseWinner(String json) {
            List<Object> list = JsonUtils.parseJsonArr(json);
            List<Map<String, Object>> maps1 = list.stream()
                    .map(map -> (String) (((Map<String, Object>) map).get("winning_bid_organization")))
                    .filter(this::isValid)
                    .flatMap(names -> Stream.of(names.split(";")))
                    .filter(this::isValid)
                    .map(name -> new HashMap<String, Object>() {{
                                put("gid", queryCompanyId(name));
                                put("name", name);
                            }}
                    )
                    .collect(Collectors.toList());
            List<Map<String, Object>> maps2 = ReUtil.findAllGroup0(Pattern.compile("\\d+(\\.\\d+)*万?元"), json)
                    .stream()
                    .map(money ->
                            new HashMap<String, Object>() {{
                                String moneyNumber = money.replaceAll("[万元]", "");
                                BigDecimal moneyDecimal = new BigDecimal(moneyNumber);
                                if (money.endsWith("万元")) {
                                    put("amount", moneyDecimal.multiply(new BigDecimal(10_000)).toPlainString());
                                } else {
                                    put("amount", moneyDecimal.toPlainString());
                                }
                            }}
                    ).collect(Collectors.toList());
            return Tuple2.of(JsonUtils.toString(maps1), JsonUtils.toString(maps2));
        }

        private String queryCompanyId(String companyName) {
            String sql = new SQL().SELECT("company_id")
                    .FROM("company_index")
                    .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                    .toString();
            String res = companyBase435.queryForObject(sql, rs -> rs.getString(1));
            return ObjUtil.defaultIfNull(res, "");
        }

        private boolean isValid(String str) {
            return StrUtil.isNotBlank(str) && !"无".equals(str);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            sink.flush();
        }

        @Override
        public void finish() {
            sink.flush();
        }

        @Override
        public void close() {
            sink.flush();
        }
    }
}
