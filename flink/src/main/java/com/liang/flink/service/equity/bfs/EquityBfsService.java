package com.liang.flink.service.equity.bfs;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.dto.*;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

import static com.liang.flink.service.equity.bfs.dto.Operation.*;
import static java.math.BigDecimal.ZERO;

@Slf4j
public class EquityBfsService {
    public static final BigDecimal PERCENT_FIVE = new BigDecimal("0.05");
    public static final BigDecimal PERCENT_TEN = new BigDecimal("0.10");
    public static final BigDecimal PERCENT_HALF = new BigDecimal("0.5");
    private static final int MAX_LEVEL = 100;
    private final EquityBfsDao dao = new EquityBfsDao();
    // the map with all shareholders
    private final Map<String, RatioPathCompanyDto> allShareholders = new HashMap<>();
    // the queue used for bfs
    private final Queue<Chain> bfsQueue = new ArrayDeque<>();

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        new EquityBfsService().bfs("2318455639");
    }

    public void bfs(String companyId) {
        // prepare
        if (!TycUtils.isUnsignedId(companyId)) return;
        String companyName = dao.queryCompanyName(companyId);
        if (!TycUtils.isValidName(companyName)) return;
        allShareholders.clear();
        bfsQueue.clear();
        int currentLevel = -1;
        // start bfs
        bfsQueue.offer(new Chain(new Node(companyId, companyName)));
        while (!bfsQueue.isEmpty() && currentLevel++ < MAX_LEVEL) {
            log.debug("开始遍历第 {} 层", currentLevel);
            int size = bfsQueue.size();
            while (size-- > 0) {
                // queue.poll()
                Chain polledChain = Objects.requireNonNull(bfsQueue.poll());
                Node polledChainLastNode = polledChain.getLast();
                log.debug("queue poll: {}", polledChainLastNode);
                // query shareholders
                List<CompanyEquityRelationDetailsDto> companyEquityRelationDetailsDtos = dao.queryShareholder(polledChainLastNode.getId());
                // queue.offer()
                for (CompanyEquityRelationDetailsDto dto : companyEquityRelationDetailsDtos) {
                    // chain archive? chain update? ratio update?
                    Operation judgeResult = judge(companyId, polledChain, dto);
                    process(judgeResult, polledChain, dto);
                }
            }
        }
        for (Map.Entry<String, RatioPathCompanyDto> entry : allShareholders.entrySet()) {
            RatioPathCompanyDto dto = entry.getValue();
            log.info("shareholder: {}({}), {}", dto.getShareholderName(), dto.getShareholderId(), dto.getTotalRatioSnapshot());
            for (Chain chain : dto.getChainsSnapshot()) {
                log.info("chain: {}", chain);
            }
        }
    }

    /**
     * `allShareholders` 在该方法中只读不写
     */
    private Operation judge(String companyId, Chain polledChain, CompanyEquityRelationDetailsDto dto) {
        String dtoShareholderId = dto.getShareholderId();
        String dtoShareholderName = dto.getShareholderName();
        BigDecimal dtoRatio = dto.getRatio();
        // 是否重复根结点
        if (companyId.equals(dtoShareholderId)) {
            return DROP;
        }
        // 是否在本条路径上出现过
        if (polledChain.getIds().contains(dtoShareholderId)) {
            return UPDATE_CHAIN_ONLY;
        }
        // 是否在其他路径上出现过
        if (allShareholders.containsKey(dtoShareholderId)) {
            return UPDATE_CHAIN_AND_RATIO;
        }
        // 是否是自然人
        if (TycUtils.isTycUniqueEntityId(dtoShareholderId) && dtoShareholderId.length() == 17) {
            return UPDATE_CHAIN_AND_RATIO;
        }
        // 股权比例是否为0
        if (ZERO.compareTo(dtoRatio) == 0) {
            return UPDATE_CHAIN_AND_RATIO;
        }
        // 其他
        return NOT_ARCHIVE;
    }

    /**
     * `allShareholders` 与 `bfsQueue` 在该方法中发生写入
     */
    private void process(Operation judgeResult, Chain polledChain, CompanyEquityRelationDetailsDto dto) {
        String dtoShareholderId = dto.getShareholderId();
        String dtoShareholderName = dto.getShareholderName();
        BigDecimal dtoRatio = dto.getRatio();
        if (judgeResult == NOT_ARCHIVE) {
            Edge newEdge = new Edge(dtoRatio, false);
            Node newNode = new Node(dtoShareholderId, dtoShareholderName);
            Chain newChain = new Chain(polledChain, newEdge, newNode);
            bfsQueue.offer(newChain);
            allShareholders.putIfAbsent(dtoShareholderId, new RatioPathCompanyDto(dtoShareholderId, dtoShareholderName));
            allShareholders.get(dtoShareholderId).addChain(newChain);
        }
        // 不加入队列
        else if (judgeResult == UPDATE_CHAIN_AND_RATIO) {
            Edge newEdge = new Edge(dtoRatio, false);
            Node newNode = new Node(dtoShareholderId, dtoShareholderName);
            Chain newChain = new Chain(polledChain, newEdge, newNode);
            allShareholders.putIfAbsent(dtoShareholderId, new RatioPathCompanyDto(dtoShareholderId, dtoShareholderName));
            allShareholders.get(dtoShareholderId).addChain(newChain);
        }
        // 不加入队列 & 虚边
        else if (judgeResult == UPDATE_CHAIN_ONLY) {
            Edge newEdge = new Edge(dtoRatio, true);
            Node newNode = new Node(dtoShareholderId, dtoShareholderName);
            Chain newChain = new Chain(polledChain, newEdge, newNode);
            allShareholders.putIfAbsent(dtoShareholderId, new RatioPathCompanyDto(dtoShareholderId, dtoShareholderName));
            allShareholders.get(dtoShareholderId).addChain(newChain);
        }
        // 啥也不干
        else if (judgeResult == DROP) {
        }
    }
}
