package com.liang.flink.service.equity.bfs;

import cn.hutool.core.util.StrUtil;
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
import java.util.stream.Collectors;

import static com.liang.flink.service.equity.bfs.dto.Operation.*;

@Slf4j
public class EquityBfsService {
    private static final BigDecimal THRESHOLD = new BigDecimal("0.000001");
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
        List<Map<String, Object>> columnMaps = new EquityBfsService().bfs("2318455639");
        columnMaps.sort(Comparator.comparing(map -> String.valueOf(map.get("shareholder_id"))));
        for (Map<String, Object> columnMap : columnMaps) {
            System.out.println(StrUtil.repeat("=", 100));
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                System.out.println(entry.getKey() + " -> " + entry.getValue());
            }
        }
    }

    public List<Map<String, Object>> bfs(Object companyGid) {
        // prepare
        this.companyId = String.valueOf(companyGid);
        if (!TycUtils.isUnsignedId(companyId)) return new ArrayList<>();
        this.companyName = dao.queryCompanyName(companyId);
        if (!TycUtils.isValidName(companyName)) return new ArrayList<>();
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
        return allShareholders
                .values()
                .stream()
                .filter(ratioPathCompanyDto ->
                        TycUtils.isUnsignedId(ratioPathCompanyDto.getCompanyId()) &&
                                TycUtils.isTycUniqueEntityId(ratioPathCompanyDto.getShareholderId()) &&
                                TycUtils.isUnsignedId(ratioPathCompanyDto.getShareholderNameId()) &&
                                TycUtils.isUnsignedId(ratioPathCompanyDto.getShareholderMasterCompanyId()) &&
                                TycUtils.isValidName(ratioPathCompanyDto.getCompanyName()) &&
                                TycUtils.isValidName(ratioPathCompanyDto.getShareholderName()))
                .map(RatioPathCompanyDto::toColumnMap).collect(Collectors.toList());
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
        // 异常id
        if (!TycUtils.isTycUniqueEntityId(shareholderId)) {
            return DROP;
        }
        // 自然人
        if (shareholderId.length() == 17) {
            return ARCHIVE_WITH_UPDATE_PATH_AND_RATIO;
        }
        // 重复根结点
        if (companyId.equals(shareholderId)) {
            return DROP;
        }
        // 股权比例低于停止穿透的阈值
        if (THRESHOLD.compareTo(newPath.getValidRatio()) > 0) {
            return DROP;
        }
        // 注吊销
        if (!dao.isAlive(shareholderId)) {
            return DROP;
        }
        // 在本条路径上出现过
        if (polledPath.getNodeIds().contains(shareholderId)) {
            return ARCHIVE_WITH_UPDATE_PATH_ONLY;
        }
        // 001
        if (dao.is001(shareholderId)) {
            return ARCHIVE_WITH_UPDATE_PATH_AND_RATIO;
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
        BigDecimal ratio = shareholder.getRatio();
        Edge newEdge = new Edge(ratio, judgeResult == ARCHIVE_WITH_UPDATE_PATH_ONLY);
        Node newNode = new Node(shareholderId, shareholderName);
        Path newPath = Path.newPath(polledPath, newEdge, newNode);
        // 更新股东列表
        allShareholders.compute(shareholderId, (k, v) -> {
            RatioPathCompanyDto ratioPathCompanyDto;
            if (v != null) {
                ratioPathCompanyDto = v;
            } else {
                String shareholderType = (shareholderId.length() == 17) ? "2" : "1";
                Map<String, Object> shareholderInfoColumnMap = dao.queryHumanOrCompanyInfo(shareholderId, shareholderType);
                String shareholderNameId = String.valueOf(shareholderInfoColumnMap.get("name_id"));
                String shareholderMasterCompanyId = String.valueOf(shareholderInfoColumnMap.get("company_id"));
                ratioPathCompanyDto = new RatioPathCompanyDto(companyId, companyName, shareholderType, shareholderId, shareholderName, shareholderNameId, shareholderMasterCompanyId);
            }
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
}
