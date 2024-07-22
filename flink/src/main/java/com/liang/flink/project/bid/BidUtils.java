package com.liang.flink.project.bid;

import cn.hutool.core.util.ObjUtil;
import com.google.common.collect.Lists;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
public class BidUtils {
    public static Map<String, Object> parseBidInfo(String bidInfo) {
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

    private static String queryCompanyIdByCompanyName(String companyName) {
        String sql = new SQL()
                .SELECT("company_id")
                .FROM("company_index")
                .WHERE("company_name = " + SqlUtils.formatValue(companyName))
                .toString();
        String res = new JdbcTemplate("435.company_base").queryForObject(sql, rs -> rs.getString(1));
        return ObjUtil.defaultIfNull(res, "");
    }
}
