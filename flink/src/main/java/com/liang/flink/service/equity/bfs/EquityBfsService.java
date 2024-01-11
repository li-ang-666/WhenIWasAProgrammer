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
    private static final BigDecimal THRESHOLD = new BigDecimal("0.000001");
    private static final BigDecimal PERCENT_FIVE = new BigDecimal("0.05");
    private static final BigDecimal PERCENT_TEN = new BigDecimal("0.10");
    private static final BigDecimal PERCENT_HALF = new BigDecimal("0.5");
    private static final int MAX_LEVEL = 1000;
    private final EquityBfsDao dao = new EquityBfsDao();
    private final Map<String, RatioPathCompanyDto> allShareholders = new HashMap<>();
    private final Queue<Chain> bfsQueue = new ArrayDeque<>();
    private String companyId;
    private String companyName;
    private int currentLevel;

    public static void main(String[] args) {
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        new EquityBfsService().bfs("2318455639");
    }

    public void bfs(Object companyGid) {
        // prepare
        this.companyId = String.valueOf(companyGid);
        if (!TycUtils.isUnsignedId(companyId)) return;
        this.companyName = dao.queryCompanyName(companyId);
        if (!TycUtils.isValidName(companyName)) return;
        allShareholders.clear();
        bfsQueue.clear();
        currentLevel = -1;
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
                    Operation judgeResult = judgeQueriedShareholder(polledChain, dto);
                    processQueriedShareholder(polledChain, dto, judgeResult);
                }
            }
        }
        debugShareholderMap();
    }

    /**
     * `allShareholders` 在该方法中只读不写
     */
    private Operation judgeQueriedShareholder(Chain polledChain, CompanyEquityRelationDetailsDto dto) {
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
    private void processQueriedShareholder(Chain polledChain, CompanyEquityRelationDetailsDto dto, Operation judgeResult) {
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
            // 路径list & 总股比
            ratioPathCompanyDto.getChains().add(newChain);
            ratioPathCompanyDto.setTotalValidRatio(ratioPathCompanyDto.getTotalValidRatio().add(newChain.getValidRatio()));
            // 是否某条路径终点
            if (!ratioPathCompanyDto.isEnd()) {
                ratioPathCompanyDto.setEnd(judgeResult != NOT_ARCHIVE);
            }
            // 是否直接股东 & 直接股比
            if (currentLevel == 0) {
                ratioPathCompanyDto.setDirectShareholder(true);
                ratioPathCompanyDto.setDirectRatio(new BigDecimal(ratioPathCompanyDto.getTotalValidRatio().toPlainString()));
            }
            return ratioPathCompanyDto;
        });
        if (judgeResult == NOT_ARCHIVE) {
            bfsQueue.offer(newChain);
        }
    }

    private void debugShareholderMap() {
        allShareholders.entrySet()
                .stream()
                .sorted((o1, o2) -> o2.getValue().getTotalValidRatio().compareTo(o1.getValue().getTotalValidRatio()))
                .forEach(e -> {
                    RatioPathCompanyDto dto = e.getValue();
                    log.debug("shareholder: {}({}), {}", dto.getShareholderName(), dto.getShareholderId(), dto.getTotalValidRatio().setScale(12, DOWN).stripTrailingZeros().toPlainString());
                    dto.getChains()
                            .stream()
                            .sorted((o1, o2) -> o2.getValidRatio().compareTo(o1.getValidRatio()))
                            .forEach(chain -> {
                                String debugString = chain.toDebugString();
                                log.debug("chain: {}", debugString);
                            });
                });
    }
}
