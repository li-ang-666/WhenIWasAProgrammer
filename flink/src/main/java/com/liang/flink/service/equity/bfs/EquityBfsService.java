package com.liang.flink.service.equity.bfs;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.dto.Operation;
import com.liang.flink.service.equity.bfs.dto.mysql.CompanyEquityRelationDetailsDto;
import com.liang.flink.service.equity.bfs.dto.mysql.RatioPathCompanyDto;
import com.liang.flink.service.equity.bfs.dto.pojo.Edge;
import com.liang.flink.service.equity.bfs.dto.pojo.Node;
import com.liang.flink.service.equity.bfs.dto.pojo.Path;
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
    private final Queue<Path> bfsQueue = new ArrayDeque<>();
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
        bfsQueue.offer(Path.newPath(new Node(companyId, companyName)));
        while (!bfsQueue.isEmpty() && currentLevel++ < MAX_LEVEL) {
            log.debug("开始遍历第 {} 层", currentLevel);
            int size = bfsQueue.size();
            while (size-- > 0) {
                // queue.poll()
                Path polledPath = Objects.requireNonNull(bfsQueue.poll());
                Node polledPathLastNode = polledPath.getLast();
                log.debug("queue poll: {}", polledPathLastNode);
                // query shareholders
                List<CompanyEquityRelationDetailsDto> shareholders = dao.queryShareholder(polledPathLastNode.getId());
                // queue.offer()
                for (CompanyEquityRelationDetailsDto shareholder : shareholders) {
                    // chain archive? chain update? ratio update?
                    Operation judgeResult = judgeQueriedShareholder(polledPath, shareholder);
                    processQueriedShareholder(polledPath, shareholder, judgeResult);
                }
            }
        }
        debugShareholderMap();
    }

    /**
     * `allShareholders` 在该方法中只读不写
     */
    private Operation judgeQueriedShareholder(Path polledPath, CompanyEquityRelationDetailsDto shareholder) {
        String shareholderId = shareholder.getShareholderId();
        String shareholderName = shareholder.getShareholderName();
        BigDecimal ratio = shareholder.getRatio();
        Edge newEdge = new Edge(ratio, false);
        Node newNode = new Node(shareholderId, shareholderName);
        Path newPath = Path.newPath(polledPath, newEdge, newNode);
        // 是否重复根结点
        if (companyId.equals(shareholderId)) {
            return DROP;
        }
        // 股权比例是否到达停止穿透的阈值
        if (THRESHOLD.compareTo(newPath.getValidRatio()) > 0) {
            return DROP;
        }
        // 是否在本条路径上出现过
        if (polledPath.getNodeIds().contains(shareholderId)) {
            return UPDATE_CHAIN_ONLY;
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
    private void processQueriedShareholder(Path polledPath, CompanyEquityRelationDetailsDto shareholder, Operation judgeResult) {
        if (judgeResult == DROP) {
            return;
        }
        String shareholderId = shareholder.getShareholderId();
        String shareholderName = shareholder.getShareholderName();
        String shareholderNameId = shareholder.getShareholderNameId();
        BigDecimal ratio = shareholder.getRatio();
        Edge newEdge = new Edge(ratio, judgeResult == UPDATE_CHAIN_ONLY);
        Node newNode = new Node(shareholderId, shareholderName);
        Path newPath = Path.newPath(polledPath, newEdge, newNode);
        allShareholders.compute(shareholderId, (k, v) -> {
            RatioPathCompanyDto ratioPathCompanyDto = (v != null) ? v : new RatioPathCompanyDto(shareholderId, shareholderName, shareholderNameId);
            // 路径case & 总股比
            ratioPathCompanyDto.getPaths().add(newPath);
            ratioPathCompanyDto.setTotalValidRatio(ratioPathCompanyDto.getTotalValidRatio().add(newPath.getValidRatio()));
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
            bfsQueue.offer(newPath);
        }
    }

    private void debugShareholderMap() {
        allShareholders.entrySet()
                .stream()
                .sorted((o1, o2) -> o2.getValue().getTotalValidRatio().compareTo(o1.getValue().getTotalValidRatio()))
                .forEach(e -> {
                    RatioPathCompanyDto dto = e.getValue();
                    log.debug("shareholder: {}({}), {}", dto.getShareholderName(), dto.getShareholderId(), dto.getTotalValidRatio().setScale(12, DOWN).stripTrailingZeros().toPlainString());
                    dto.getPaths()
                            .stream()
                            .sorted((o1, o2) -> o2.getValidRatio().compareTo(o1.getValidRatio()))
                            .forEach(path -> {
                                String debugString = path.toDebugString();
                                log.debug("chain: {}", debugString);
                            });
                });
    }
}
