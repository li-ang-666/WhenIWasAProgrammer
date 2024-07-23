package com.liang.flink.project.bid;

import cn.hutool.core.util.ObjUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.google.common.collect.Lists;
import com.liang.common.dto.Config;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
@SuppressWarnings("unchecked")
public class BidService {
    private static final int TIMEOUT = (int) TimeUnit.HOURS.toMillis(24);
    private final JdbcTemplate companyBase435 = new JdbcTemplate("435.company_base");
    private final JdbcTemplate dataBid104 = new JdbcTemplate("104.data_bid");

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig("");
        ConfigUtils.setConfig(config);
        String uuid = "cfa21355d86341f1915c7d005e7b91a5";
        BidService service = new BidService();
        Map<String, Object> columnMap = service.queryCompanyBidAll(uuid);
        Map<String, Object> postResultMap = service.post(uuid, (String) columnMap.get("title"), (String) columnMap.get("content"), (String) columnMap.get("type"));
        postResultMap.forEach((k, v) -> System.out.println(k + " -> " + v));
    }

    public Map<String, Object> post(String uuid, String title, String content, String type) {
        HttpRequest httpRequest = HttpUtil.createPost("http://10.100.6.45:9999/bid_rank")
                .timeout(TIMEOUT)
                .form("text", content)
                .form("bid_uuid", uuid)
                .form("doc_type", type)
                .form("title", title);
        try (HttpResponse httpResponse = httpRequest.execute()) {
            List<Map<String, Object>> biddingUnitResult = new ArrayList<>();
            List<Map<String, Object>> tenderingProxyAgentResult = new ArrayList<>();
            List<String> bidSubmissionDeadlineResult = new ArrayList<>();
            List<String> tenderDocumentAcquisitionDeadlineResult = new ArrayList<>();
            List<String> projectNumberResult = new ArrayList<>();
            if (httpResponse.getStatus() != 200) {
                throw new RuntimeException();
            }
            String result = httpResponse.body();
            log.debug("http result: {}", result);
            Map<String, Object> resultMap = JsonUtils.parseJsonObj(result);
            // 招标单位
            List<Map<String, Object>> biddingUnit = (List<Map<String, Object>>) (resultMap.getOrDefault("bidding_unit", new ArrayList<>()));
            for (Map<String, Object> map : biddingUnit) {
                biddingUnitResult.add(new HashMap<String, Object>() {{
                    put("name", map.getOrDefault("company_name", ""));
                    put("gid", map.getOrDefault("company_gid", ""));
                }});
            }
            // 代理单位
            List<Map<String, Object>> tenderingProxyAgent = (List<Map<String, Object>>) (resultMap.getOrDefault("tendering_proxy_agent", new ArrayList<>()));
            for (Map<String, Object> map : tenderingProxyAgent) {
                tenderingProxyAgentResult.add(new HashMap<String, Object>() {{
                    put("name", map.getOrDefault("company_name", ""));
                    put("gid", map.getOrDefault("company_gid", ""));
                }});
            }
            // 招标截止时间
            List<String> bidSubmissionDeadline = (List<String>) (resultMap.getOrDefault("bid_submission_deadline", new ArrayList<>()));
            bidSubmissionDeadlineResult.addAll(bidSubmissionDeadline);
            // 标书下载截止时间
            List<String> tenderDocumentAcquisitionDeadline = (List<String>) (resultMap.getOrDefault("tender_document_acquisition_deadline", new ArrayList<>()));
            tenderDocumentAcquisitionDeadlineResult.addAll(tenderDocumentAcquisitionDeadline);
            // 项目编号
            List<String> projectNumber = (List<String>) (resultMap.getOrDefault("project_number", new ArrayList<>()));
            projectNumberResult.addAll(projectNumber);
            // 返回
            Map<String, Object> returnMap = new HashMap<>();
            returnMap.put("bidding_unit", biddingUnitResult.isEmpty() ? "" : "[" + JsonUtils.toString(biddingUnitResult) + "]");
            returnMap.put("tendering_proxy_agent", tenderingProxyAgentResult.isEmpty() ? "" : "[" + JsonUtils.toString(tenderingProxyAgentResult) + "]");
            returnMap.put("bid_submission_deadline", bidSubmissionDeadlineResult.isEmpty() ? "" : bidSubmissionDeadlineResult.get(0));
            returnMap.put("tender_document_acquisition_deadline", tenderDocumentAcquisitionDeadlineResult.isEmpty() ? "" : tenderDocumentAcquisitionDeadlineResult.get(0));
            returnMap.put("project_number", projectNumberResult.isEmpty() ? "" : projectNumberResult.get(0));
            return returnMap;
        } catch (Exception e) {
            log.error("post AI error, uuid: {}", uuid);
            return new HashMap<String, Object>() {{
                put("bidding_unit", "");
                put("tendering_proxy_agent", "");
                put("bid_submission_deadline", "");
                put("tender_document_acquisition_deadline", "");
                put("project_number", "");
            }};
        }
    }

    public Map<String, Object> parseBidInfo(String bidInfo) {
        Map<String, Object> columnMap = new LinkedHashMap<>();
        // prepare
        List<String> contractNos = Lists.newArrayList();
        List<Map<String, Object>> candidates = Lists.newArrayList();
        List<Map<String, Object>> winners = Lists.newArrayList();
        List<Map<String, Object>> winnerRawAmounts = Lists.newArrayList();
        List<Map<String, Object>> winnerAmounts = Lists.newArrayList();
        List<Map<String, Object>> budgetRawAmounts = Lists.newArrayList();
        List<Map<String, Object>> budgetAmounts = Lists.newArrayList();
        // parse
        List<Object> parsedObjects = ObjUtil.defaultIfNull(JsonUtils.parseJsonArr(bidInfo), new ArrayList<>());
        for (Object parsedObject : parsedObjects) {
            Map<String, Object> parsedMap = (Map<String, Object>) parsedObject;
            // contract_no
            contractNos.add(String.valueOf(parsedMap.getOrDefault("contract_no", "")));
            // action
            List<Map<String, Object>> actionMaps = (List<Map<String, Object>>) (parsedMap.getOrDefault("action", new ArrayList<>()));
            for (Map<String, Object> actionMap : actionMaps) {
                // raw budget 来自 raw_balance(不为空)
                String rawBalance = String.valueOf(actionMap.getOrDefault("raw_balance", ""));
                if (TycUtils.isValidName(rawBalance)) {
                    budgetRawAmounts.add(new HashMap<String, Object>() {{
                        put("amount", rawBalance);
                    }});
                }
                // budget 来自 balance(不为空)
                String balance = String.valueOf(actionMap.getOrDefault("balance", ""));
                //
                if (TycUtils.isValidName(balance)) {
                    budgetAmounts.add(new HashMap<String, Object>() {{
                        put("amount", balance);
                    }});
                }
                // candidate 来自 supplier_candidate
                Map<String, Object> candidateMap = (Map<String, Object>) (actionMap.getOrDefault("supplier_candidate", new LinkedHashMap<>()));
                List<Map<String, Object>> candidateEntities = (List<Map<String, Object>>) (candidateMap.getOrDefault("entity", new ArrayList<>()));
                for (Map<String, Object> candidateEntity : candidateEntities) {
                    String name = String.valueOf(candidateEntity.getOrDefault("name", ""));
                    String rawOfferPrice = String.valueOf(candidateEntity.getOrDefault("raw_offer_price", ""));
                    String offerPrice = String.valueOf(candidateEntity.getOrDefault("offer_price", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        candidates.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                            put("raw_offer_price", rawOfferPrice);
                            put("offer_price", offerPrice);
                        }});
                    }
                }
                // winner 来自 supplier
                Map<String, Object> winnerMap = (Map<String, Object>) (actionMap.getOrDefault("supplier", new LinkedHashMap<>()));
                List<Map<String, Object>> winnerEntities = (List<Map<String, Object>>) (winnerMap.getOrDefault("entity", new ArrayList<>()));
                for (Map<String, Object> winnerEntity : winnerEntities) {
                    String name = String.valueOf(winnerEntity.getOrDefault("name", ""));
                    String rawOfferPrice = String.valueOf(winnerEntity.getOrDefault("raw_offer_price", ""));
                    String offerPrice = String.valueOf(winnerEntity.getOrDefault("offer_price", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        // winner
                        winners.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                        }});
                        // raw winner amount
                        winnerRawAmounts.add(new HashMap<String, Object>() {{
                            put("amount", rawOfferPrice);
                        }});
                        // winner amount
                        winnerAmounts.add(new HashMap<String, Object>() {{
                            put("amount", offerPrice);
                        }});
                    }
                }
            }
        }
        columnMap.put("contract_no", contractNos.isEmpty() ? "" : contractNos.get(0));
        columnMap.put("candidates", candidates.isEmpty() ? "" : "[" + JsonUtils.toString(candidates) + "]");
        columnMap.put("winners", winners.isEmpty() ? "" : "[" + JsonUtils.toString(winners) + "]");
        columnMap.put("winner_raw_amounts", winnerRawAmounts.isEmpty() ? "" : "[" + JsonUtils.toString(winnerRawAmounts) + "]");
        columnMap.put("winner_amounts", winnerAmounts.isEmpty() ? "" : "[" + JsonUtils.toString(winnerAmounts) + "]");
        columnMap.put("budget_raw_amounts", budgetRawAmounts.isEmpty() ? "" : "[" + JsonUtils.toString(budgetRawAmounts) + "]");
        columnMap.put("budget_amounts", budgetAmounts.isEmpty() ? "" : "[" + JsonUtils.toString(budgetAmounts) + "]");
        return columnMap;
    }

    private String queryCompanyIdByCompanyName(String companyName) {
        String sql = new SQL()
                .SELECT("company_id")
                .FROM("company_index")
                .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                .toString();
        String res = companyBase435.queryForObject(sql, rs -> rs.getString(1));
        return ObjUtil.defaultIfNull(res, "");
    }

    public String queryUuid(String mainId) {
        String sql = new SQL().SELECT("uuid")
                .FROM("company_bid")
                .WHERE("id = " + SqlUtils.formatValue(mainId))
                .toString();
        String res = dataBid104.queryForObject(sql, rs -> rs.getString(1));
        return ObjUtil.defaultIfNull(res, "");
    }

    public Map<String, Object> queryCompanyBidAll(String uuid) {
        String sql = new SQL().SELECT("*")
                .FROM("company_bid")
                .WHERE("uuid = " + SqlUtils.formatValue(uuid))
                .toString();
        List<Map<String, Object>> columnMaps = dataBid104.queryForColumnMaps(sql);
        return columnMaps.isEmpty() ? new HashMap<>() : columnMaps.get(0);
    }
}
