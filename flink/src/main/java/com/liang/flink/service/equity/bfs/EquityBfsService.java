package com.liang.flink.service.equity.bfs;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.dto.*;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

import static com.liang.flink.service.equity.bfs.dto.Operation.*;
import static java.math.RoundingMode.DOWN;

@Slf4j
public class EquityBfsService {
    public static final BigDecimal THRESHOLD = new BigDecimal("0.0001");
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
            if (currentLevel == 1) ;
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
                    Operation judgeResult = judgeQueriedShareholder(companyId, polledChain, dto);
                    processQueriedShareholder(judgeResult, polledChain, dto);
                }
            }
            // 第0层(root)结束后, 查到的股东(第1层), 都是直接股东
            if (currentLevel == 0) registerDirectShareholder();
        }
        debugShareholderMap();
    }

    /**
     * `allShareholders` 在该方法中只读不写
     */
    private Operation judgeQueriedShareholder(String companyId, Chain polledChain, CompanyEquityRelationDetailsDto dto) {
        String shareholderId = dto.getShareholderId();
        String shareholderName = dto.getShareholderName();
        BigDecimal ratio = dto.getRatio();
        Edge newEdge = new Edge(ratio, false);
        Node newNode = new Node(shareholderId, shareholderName);
        Chain newChain = new Chain(polledChain, newEdge, newNode);
        // 是否重复根结点
        if (companyId.equals(shareholderId)) {
            return DROP;
        }
        // 直接股权比例是否到达停止穿透的阈值
        if (THRESHOLD.compareTo(ratio) > 0) {
            return DROP;
        }
        // 间接股权比例是否到达停止穿透的阈值
        if (THRESHOLD.compareTo(newChain.getValidRatio()) > 0) {
            return DROP;
        }
        // 是否在本条路径上出现过
        if (polledChain.getIds().contains(shareholderId)) {
            return UPDATE_CHAIN_ONLY;
        }
        // 是否在其他路径上出现过
        if (allShareholders.containsKey(shareholderId)) {
            return UPDATE_CHAIN_AND_RATIO;
        }
        // 是否是自然人
        if (TycUtils.isTycUniqueEntityId(shareholderId) && shareholderId.length() == 17) {
            return UPDATE_CHAIN_AND_RATIO;
        }
        // 其他
        return NOT_ARCHIVE;
    }

    /**
     * `allShareholders` 与 `bfsQueue` 在该方法中发生写入
     */
    private void processQueriedShareholder(Operation judgeResult, Chain polledChain, CompanyEquityRelationDetailsDto dto) {
        if (judgeResult == DROP) {
            return;
        }
        String shareholderId = dto.getShareholderId();
        String shareholderName = dto.getShareholderName();
        String shareholderNameId = dto.getShareholderNameId();
        BigDecimal ratio = dto.getRatio();
        Edge newEdge = new Edge(ratio, judgeResult == UPDATE_CHAIN_ONLY);
        Node newNode = new Node(shareholderId, shareholderName);
        Chain newChain = new Chain(polledChain, newEdge, newNode);
        allShareholders.compute(shareholderId, (k, v) -> {
            RatioPathCompanyDto ratioPathCompanyDto = (v != null) ? v : new RatioPathCompanyDto(shareholderId, shareholderName, shareholderNameId);
            ratioPathCompanyDto.getChains().add(newChain);
            ratioPathCompanyDto.setTotalValidRatio(ratioPathCompanyDto.getTotalValidRatio().add(newChain.getValidRatio()));
            return ratioPathCompanyDto;
        });
        if (judgeResult == NOT_ARCHIVE) {
            bfsQueue.offer(newChain);
        }
    }

    /**
     * 直接股东
     */
    private void registerDirectShareholder() {
        for (Map.Entry<String, RatioPathCompanyDto> entry : allShareholders.entrySet()) {
            RatioPathCompanyDto dto = entry.getValue();
            dto.setDirectShareholder(true);
            dto.setDirectRatio(new BigDecimal(dto.getTotalValidRatio().toPlainString()));
        }
    }

    private void debugShareholderMap() {
        allShareholders.entrySet().stream()
                .sorted((o1, o2) -> o2.getValue().getTotalValidRatio().compareTo(o1.getValue().getTotalValidRatio()))
                .forEach(e -> {
                    RatioPathCompanyDto dto = e.getValue();
                    log.debug("shareholder: {}({}), {}", dto.getShareholderName(), dto.getShareholderId(), dto.getTotalValidRatio().setScale(12, DOWN).toPlainString());
                    dto.getChains().stream()
                            .sorted((o1, o2) -> o2.getValidRatio().compareTo(o1.getValidRatio()))
                            .forEach(chain -> log.debug("chain: {}", chain.toDebugString()));
                });
    }
}
