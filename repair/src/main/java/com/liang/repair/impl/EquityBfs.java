package com.liang.repair.impl;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.liang.common.service.SQL;
import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.SqlUtils;
import com.liang.common.util.TycUtils;
import com.liang.repair.service.ConfigHolder;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class EquityBfs extends ConfigHolder {
    public static void main(String[] args) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate("430.graph_data");
        LinkedList<Chain> bfsQueue = new LinkedList<>();
        //股东链路明细
        LinkedHashMap<String, LinkedHashSet<Chain>> shareholder2Chains = new LinkedHashMap<>();
        //股东持股明细
        LinkedHashMap<String, BigDecimal> shareholder2Ratio = new LinkedHashMap<>();
        String root = "154213205";
        bfsQueue.addLast(new Chain(new Node(root, new BigDecimal(1))));
        int level = 1;
        while (level <= 100 && !bfsQueue.isEmpty()) {
            for (int i = 0; i < bfsQueue.size(); i++) {
                Chain chain = bfsQueue.removeFirst();
                Node chainLast = chain.getLast();
                String sql = new SQL()
                        .SELECT("tyc_unique_entity_id_investor", "equity_ratio")
                        .FROM("company_equity_relation_details")
                        .WHERE("equity_ratio >= 0.05")
                        .WHERE("reference_pt_year = 2023")
                        // 排除root成环情况
                        .WHERE("company_id_investor <> " + SqlUtils.formatValue(root))
                        .WHERE("company_id_invested = " + SqlUtils.formatValue(chainLast.getId()))
                        .toString();
                List<Node> chainNewShareholders = jdbcTemplate.queryForList(sql, rs -> new Node(rs.getString(1), rs.getBigDecimal(2)));
                // 5%
                BigDecimal ChainRatio = chain.getRatio();
                for (Node chainNewShareholder : chainNewShareholders) {
                    // 链路是否归档(不再继续穿透)
                    boolean archive;
                    // 加入or不加入新股东后的链路
                    Chain newChain;
                    // 要加入的新股权比例
                    BigDecimal newRatio;
                    // 加入该股东后链路小于5%, 更新链路❌, 更新持股❌
                    if (ChainRatio.multiply(chainNewShareholder.getRatio()).compareTo(new BigDecimal("0.05")) < 0) {
                        archive = true;
                        newChain = chain.copy();
                        newRatio = new BigDecimal(0);
                    }
                    // 非root成环, 更新链路✅, 更新持股❌
                    else if (chain.containsId(chainNewShareholder.getId())) {
                        archive = true;
                        newChain = chain.copy().add(chainNewShareholder);
                        newRatio = new BigDecimal(0);
                    }
                    // 不成环但不是第一次遇到该股东, 更新链路✅, 更新持股✅
                    else if (shareholder2Chains.containsKey(chainNewShareholder.getId()) || shareholder2Ratio.containsKey(chainNewShareholder.getId())) {
                        archive = true;
                        newChain = chain.copy().add(chainNewShareholder);
                        newRatio = newChain.getRatio();
                    }
                    // 是自然人、001、注吊销, 更新链路✅, 更新持股✅
                    else if (TycUtils.isTycUniqueEntityId(chainNewShareholder.getId()) && chainNewShareholder.getId().length() == 17) {
                        archive = true;
                        newChain = chain.copy().add(chainNewShareholder);
                        newRatio = newChain.getRatio();
                    }
                    // 其他正常情况
                    else {
                        archive = false;
                        newChain = chain.copy().add(chainNewShareholder);
                        newRatio = newChain.getRatio();
                    }
                    shareholder2Chains.putIfAbsent(newChain.getLast().getId(), new LinkedHashSet<>());
                    shareholder2Chains.get(newChain.getLast().getId()).add(newChain);
                    if (shareholder2Ratio.containsKey(newChain.getLast().getId())) {
                        shareholder2Ratio.put(newChain.getLast().getId(), shareholder2Ratio.get(newChain.getLast().getId()).add(newRatio));
                    } else {
                        shareholder2Ratio.put(newChain.getLast().getId(), newRatio);
                    }
                    if (!archive) {
                        bfsQueue.addLast(newChain);
                    }
                    level++;
                }
            }
        }
        System.out.println(JsonUtils.toString(shareholder2Chains));
        System.out.println(JsonUtils.toString(shareholder2Ratio));
    }

    @Data
    @AllArgsConstructor
    private final static class Node implements Serializable {
        private String id;
        private BigDecimal ratio;
    }

    @Data
    private final static class Chain implements Serializable {
        private final LinkedList<Node> nodes = new LinkedList<>();

        public Chain(Node node) {
            this.nodes.addLast(node);
        }

        @JsonIgnore
        public BigDecimal getRatio() {
            AtomicReference<BigDecimal> bigDecimal = new AtomicReference<>(new BigDecimal(1));
            nodes.forEach(e -> bigDecimal.set(bigDecimal.get().multiply(e.getRatio())));
            return bigDecimal.get();
        }

        public boolean containsId(String id) {
            return nodes.stream().anyMatch(e -> e.getId().equals(id));
        }

        @JsonIgnore
        public Node getLast() {
            return nodes.getLast();
        }

        public Chain copy() {
            return SerializationUtils.clone(this);
        }

        public Chain add(Node node) {
            nodes.addLast(node);
            return this;
        }
    }
}
