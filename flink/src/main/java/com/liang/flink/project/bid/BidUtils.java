package com.liang.flink.project.bid;

import cn.hutool.core.io.IoUtil;
import cn.hutool.core.util.ObjUtil;
import com.google.common.collect.Lists;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
public class BidUtils {
    private static final Map<String, String> COMPANY_ID_MAPPING = new HashMap<>();

    static {
        InputStream resource = BidUtils.class.getClassLoader().getResourceAsStream("company_id_mapping.csv");
        String lines = IoUtil.read(resource, StandardCharsets.UTF_8);
        for (String line : lines.split("\n")) {
            if (line.matches("\\d+,\\d+")) {
                String[] split = line.split(",");
                COMPANY_ID_MAPPING.put(split[0], split[1]);
            }
        }
        log.info("load {} lines from company_id_mapping.csv", COMPANY_ID_MAPPING.size());
    }

    public static Map<String, Object> parseBidInfo(String bidInfo) {
        Map<String, Object> columnMap = new LinkedHashMap<>();
        // prepare
        List<String> itemNos = Lists.newArrayList();
        List<String> contractNos = Lists.newArrayList();
        List<Map<String, Object>> purchasers = Lists.newArrayList();
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
            // item_no
            itemNos.add(String.valueOf(parsedMap.getOrDefault("item_no", "")));
            // contract_no
            contractNos.add(String.valueOf(parsedMap.getOrDefault("contract_no", "")));
            // action
            List<Map<String, Object>> actionMaps = (List<Map<String, Object>>) (parsedMap.getOrDefault("action", new ArrayList<>()));
            for (Map<String, Object> actionMap : actionMaps) {
                // raw budget
                String rawBalance = String.valueOf(actionMap.getOrDefault("raw_balance", ""));
                // 预算, 不为空就保留
                if (TycUtils.isValidName(rawBalance)) {
                    budgetRawAmounts.add(new HashMap<String, Object>() {{
                        put("amount", rawBalance);
                    }});
                }
                // budget
                String balance = String.valueOf(actionMap.getOrDefault("balance", ""));
                // 预算, 不为空就保留
                if (TycUtils.isValidName(balance)) {
                    budgetAmounts.add(new HashMap<String, Object>() {{
                        put("amount", balance);
                    }});
                }
                // purchaser
                List<Map<String, Object>> clientMaps = (List<Map<String, Object>>) (actionMap.getOrDefault("client", new ArrayList<>()));
                for (Map<String, Object> clientMap : clientMaps) {
                    String name = String.valueOf(clientMap.getOrDefault("name", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        purchasers.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                        }});
                    }
                }
                // candidate
                Map<String, Object> candidateMap = (Map<String, Object>) (actionMap.getOrDefault("supplier_candidate", new LinkedHashMap<>()));
                List<Map<String, Object>> candidateEntities = (List<Map<String, Object>>) (candidateMap.getOrDefault("entity", new ArrayList<>()));
                for (Map<String, Object> candidateEntity : candidateEntities) {
                    String name = String.valueOf(candidateEntity.getOrDefault("name", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        candidates.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                            put("raw_offer_price", String.valueOf(candidateEntity.getOrDefault("raw_offer_price", "")));
                            put("offer_price", String.valueOf(candidateEntity.getOrDefault("offer_price", "")));
                        }});
                    }
                }
                // winner
                Map<String, Object> winnerMap = (Map<String, Object>) (actionMap.getOrDefault("supplier", new LinkedHashMap<>()));
                List<Map<String, Object>> winnerEntities = (List<Map<String, Object>>) (winnerMap.getOrDefault("entity", new ArrayList<>()));
                for (Map<String, Object> winnerEntity : winnerEntities) {
                    String name = String.valueOf(winnerEntity.getOrDefault("name", ""));
                    // 只要名字正确, 就保留
                    if (TycUtils.isValidName(name)) {
                        // winner
                        winners.add(new LinkedHashMap<String, Object>() {{
                            put("gid", queryCompanyIdByCompanyName(name));
                            put("name", name);
                        }});
                        // raw winner amount
                        winnerRawAmounts.add(new HashMap<String, Object>() {{
                            put("amount", String.valueOf(winnerEntity.getOrDefault("raw_offer_price", "")));
                        }});
                        // winner amount
                        winnerAmounts.add(new HashMap<String, Object>() {{
                            put("amount", String.valueOf(winnerEntity.getOrDefault("offer_price", "")));
                        }});
                    }
                }
            }
        }
        columnMap.put("item_no", itemNos.isEmpty() ? "" : itemNos.get(0));
        columnMap.put("contract_no", contractNos.isEmpty() ? "" : contractNos.get(0));
        columnMap.put("purchasers", purchasers.isEmpty() ? "" : JsonUtils.toString(purchasers));
        columnMap.put("candidates", candidates.isEmpty() ? "" : JsonUtils.toString(candidates));
        columnMap.put("winners", winners.isEmpty() ? "" : JsonUtils.toString(winners));
        columnMap.put("winner_raw_amounts", winnerRawAmounts.isEmpty() ? "" : JsonUtils.toString(winnerRawAmounts));
        columnMap.put("winner_amounts", winnerAmounts.isEmpty() ? "" : JsonUtils.toString(winnerAmounts));
        columnMap.put("budget_raw_amounts", budgetRawAmounts.isEmpty() ? "" : JsonUtils.toString(budgetRawAmounts));
        columnMap.put("budget_amounts", budgetAmounts.isEmpty() ? "" : JsonUtils.toString(budgetAmounts));
        return columnMap;
    }

    private static String queryCompanyIdByCompanyName(String companyName) {
        String sql = new SQL()
                .SELECT("company_id")
                .FROM("company_index")
                .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                .toString();
        String res = new JdbcTemplate("435.company_base").queryForObject(sql, rs -> rs.getString(1));
        return res != null ? COMPANY_ID_MAPPING.getOrDefault(res, res) : "";
    }
}
