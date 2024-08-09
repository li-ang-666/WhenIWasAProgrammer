package com.liang.flink.service.equity.bfs;

import cn.hutool.core.util.ObjUtil;
import cn.hutool.core.util.StrUtil;
import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.dto.Operation;
import com.liang.flink.service.equity.bfs.dto.ShareholderJudgeInfo;
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
    private static final BigDecimal THRESHOLD_BFS = new BigDecimal("0.000001");
    private static final BigDecimal THRESHOLD_SINK = new BigDecimal("0.01");
    private final EquityBfsDao dao = new EquityBfsDao();
    private final Map<String, RatioPathCompanyDto> allShareholders = new HashMap<>();
    private final Queue<Path> bfsQueue = new ArrayDeque<>();
    private final Map<String, List<CompanyEquityRelationDetailsDto>> investedCompanyId2Shareholders = new HashMap<>();
    private final Map<String, ShareholderJudgeInfo> shareholderJudgeInfoMap = new HashMap<>();
    // base info
    private String companyId;
    private String companyName;
    private boolean companyIsListed;
    private String companyUscc;
    private String companyOrgType;
    private String companyEntityProperty;
    private int currentScanLevel;

    public static void main(String[] args) throws Exception {
        Config config = ConfigUtils.createConfig(null);
        ConfigUtils.setConfig(config);
        List<Map<String, Object>> columnMaps = new EquityBfsService().bfs("109234464");
        columnMaps.sort(Comparator.comparing(map -> String.valueOf(map.get("shareholder_id"))));
        for (Map<String, Object> columnMap : columnMaps) {
            log.info(StrUtil.repeat("=", 100));
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                log.info("{} -> {}", entry.getKey(), entry.getValue());
            }
        }
        log.info("size: {}", columnMaps.size());
    }

    public List<Map<String, Object>> bfs(Object companyGid) {
        // prepare
        this.companyId = String.valueOf(companyGid);
        if (!TycUtils.isUnsignedId(companyId)) return new ArrayList<>();
        Map<String, Object> companyInfo = dao.queryCompanyInfo(companyId);
        if (companyInfo.isEmpty()) return new ArrayList<>();
        this.companyName = String.valueOf(companyInfo.getOrDefault("company_name", ""));
        if (!TycUtils.isValidName(companyName)) return new ArrayList<>();
        this.companyUscc = String.valueOf(companyInfo.getOrDefault("unified_social_credit_code", ""));
        this.companyOrgType = String.valueOf(companyInfo.getOrDefault("org_type", ""));
        this.companyIsListed = dao.isListed(companyId);
        this.companyEntityProperty = ObjUtil.defaultIfNull(dao.queryEntityProperty(companyId), "");
        allShareholders.clear();
        bfsQueue.clear();
        investedCompanyId2Shareholders.clear();
        shareholderJudgeInfoMap.clear();
        currentScanLevel = 0;
        // start bfs
        bfsQueue.offer(Path.newPath(new Node(companyId, companyName)));
        while (!bfsQueue.isEmpty()) {
            log.debug("开始遍历第 {} 层", currentScanLevel);
            // query this level new company-to-shareholders
            findNewInvestedCompany2Shareholders();
            // query this level new shareholder-to-judge-info
            findNewShareholderJudgeInfo();
            int size = bfsQueue.size();
            while (size-- > 0) {
                // queue.poll()
                Path polledPath = Objects.requireNonNull(bfsQueue.poll());
                Node polledPathLastNode = polledPath.getLast();
                log.debug("queue poll: {}", polledPathLastNode);
                // get shareholders from map
                List<CompanyEquityRelationDetailsDto> shareholders = investedCompanyId2Shareholders.getOrDefault(polledPathLastNode.getId(), new ArrayList<>());
                // no more shareholders
                if (shareholders.isEmpty()) {
                    processNoMoreShareholder(polledPath);
                }
                // queue.offer()
                for (CompanyEquityRelationDetailsDto shareholder : shareholders) {
                    // chain archive? chain update? ratio update?
                    Operation judgeResult = judgeQueriedShareholder(polledPath, shareholder);
                    processQueriedShareholder(polledPath, shareholder, judgeResult);
                }
            }
            currentScanLevel++;
        }
        return allShareholders2ColumnMaps();
    }

    // query this level new company-to-shareholders
    private void findNewInvestedCompany2Shareholders() {
        Set<String> newInvestedCompanyIds = bfsQueue.parallelStream()
                .map(investedCompanyPath -> investedCompanyPath.getLast().getId())
                .filter(investedCompanyId -> TycUtils.isUnsignedId(investedCompanyId) && !investedCompanyId2Shareholders.containsKey(investedCompanyId))
                .collect(Collectors.toSet());
        if (!newInvestedCompanyIds.isEmpty()) {
            investedCompanyId2Shareholders.putAll(dao.queryThisLevelShareholder(newInvestedCompanyIds));
        }
    }

    // query this level new shareholder-judge-info
    private void findNewShareholderJudgeInfo() {
        Set<String> newShareholders = investedCompanyId2Shareholders.values().parallelStream()
                .flatMap(shareholders -> shareholders.parallelStream().map(CompanyEquityRelationDetailsDto::getShareholderId))
                .filter(shareholderId -> TycUtils.isUnsignedId(shareholderId) && !shareholderJudgeInfoMap.containsKey(shareholderId))
                .collect(Collectors.toSet());
        if (!newShareholders.isEmpty()) {
            shareholderJudgeInfoMap.putAll(dao.queryShareholderJudgeInfo(newShareholders));
        }
    }

    // `allShareholders` 在该方法中只读不写
    private Operation judgeQueriedShareholder(Path polledPath, CompanyEquityRelationDetailsDto shareholder) {
        String shareholderId = shareholder.getShareholderId();
        String shareholderName = shareholder.getShareholderName();
        BigDecimal ratio = shareholder.getRatio();
        Edge newEdge = new Edge(ratio, true);
        Node newNode = new Node(shareholderId, shareholderName);
        Path newPath = Path.newPath(polledPath, newEdge, newNode);
        ShareholderJudgeInfo shareholderJudgeInfo = shareholderJudgeInfoMap
                .getOrDefault(shareholderId, new ShareholderJudgeInfo(shareholderId, false, false));
        // 异常id
        if (!TycUtils.isTycUniqueEntityId(shareholderId)) {
            return DROP;
        }
        // 重复根结点
        if (companyId.equals(shareholderId)) {
            return DROP;
        }
        // 链路总股权比例低于停止穿透的阈值
        if (THRESHOLD_BFS.compareTo(newPath.getValidRatio()) > 0) {
            return DROP;
        }
        // 注吊销公司
        if (shareholderJudgeInfo.isClosed()) {
            return DROP;
        }
        // 在本条路径上出现过(成环)
        if (polledPath.getNodeIds().contains(shareholderId)) {
            return ARCHIVE_WITH_UPDATE_PATH_ONLY;
        }
        // 001
        if (shareholderJudgeInfo.is001()) {
            return ARCHIVE_WITH_UPDATE_PATH_AND_RATIO;
        }
        // 自然人
        if (shareholderId.length() == 17) {
            return ARCHIVE_WITH_UPDATE_PATH_AND_RATIO;
        }
        // 其他
        return NOT_ARCHIVE;
    }

    // `allShareholders` 与 `bfsQueue` 在该方法中发生写入
    private void processQueriedShareholder(Path polledPath, CompanyEquityRelationDetailsDto shareholder, Operation judgeResult) {
        if (judgeResult == DROP) {
            return;
        }
        // 构造newPath
        String shareholderId = shareholder.getShareholderId();
        String shareholderName = shareholder.getShareholderName();
        BigDecimal ratio = shareholder.getRatio();
        Edge newEdge = new Edge(ratio, judgeResult != ARCHIVE_WITH_UPDATE_PATH_ONLY);
        Node newNode = new Node(shareholderId, shareholderName);
        Path newPath = Path.newPath(polledPath, newEdge, newNode);
        // 更新股东列表
        allShareholders.compute(shareholderId, (k, v) -> {
            RatioPathCompanyDto ratioPathCompanyDto;
            if (v != null) {
                ratioPathCompanyDto = v;
            } else {
                // 确认股东最新信息, 放在过滤1%股东后再做
                ratioPathCompanyDto = new RatioPathCompanyDto(companyId, companyName, companyIsListed, companyUscc, companyOrgType, companyEntityProperty, shareholderId, currentScanLevel + 1);
                ratioPathCompanyDto.setShareholderName(shareholderName);
            }
            // 新路径 & 总股比
            ratioPathCompanyDto.getPaths().add(newPath);
            ratioPathCompanyDto.setTotalValidRatio(ratioPathCompanyDto.getTotalValidRatio().add(newPath.getValidRatio()));
            // 是否某条路径终点
            if (!ratioPathCompanyDto.isEnd()) {
                // 成环而终止的公司, 不应该拥有候选实控人的权利
                ratioPathCompanyDto.setEnd(judgeResult != NOT_ARCHIVE && judgeResult != ARCHIVE_WITH_UPDATE_PATH_ONLY);
            }
            // 是否直接股东 & 直接股比
            if (currentScanLevel == 0) {
                ratioPathCompanyDto.setDirectShareholder(true);
                ratioPathCompanyDto.setDirectRatio(ratioPathCompanyDto.getTotalValidRatio());
            }
            // 最后一次出现层数
            ratioPathCompanyDto.setShareholderLastAppearLevel(currentScanLevel + 1);
            return ratioPathCompanyDto;
        });
        if (judgeResult == NOT_ARCHIVE) {
            bfsQueue.offer(newPath);
        }
    }

    // `allShareholders` 在该方法中发生写入
    private void processNoMoreShareholder(Path polledPath) {
        // 无任何投资关系
        if (currentScanLevel == 0) {
            allShareholders.put(companyId, getSpecialRatioPathCompanyDto());
        }
        // 股权穿透尽头
        else {
            allShareholders.get(polledPath.getLast().getId()).setEnd(true);
        }
    }

    // 有可能因为 Operation.DROP 导致 allShareholders 为 empty
    private List<Map<String, Object>> allShareholders2ColumnMaps() {
        Set<String> humanShareholderIds = new HashSet<>();
        Set<String> companyShareholderIds = new HashSet<>();
        // 过滤三层以内或者比例大于1%的股东, 区分自然人和公司
        for (Map.Entry<String, RatioPathCompanyDto> entry : allShareholders.entrySet()) {
            String shareholderId = entry.getKey();
            RatioPathCompanyDto ratioPathCompanyDto = entry.getValue();
            if (ratioPathCompanyDto.getShareholderFirstAppearLevel() > 3 && ratioPathCompanyDto.getTotalValidRatio().compareTo(THRESHOLD_SINK) < 0) {
                continue;
            }
            if (TycUtils.isUnsignedId(shareholderId)) {
                companyShareholderIds.add(shareholderId);
            } else {
                humanShareholderIds.add(shareholderId);
            }
        }
        // 查询所有股东最新信息
        Map<String, Map<String, Object>> allShareholderInfoMap = new HashMap<>();
        if (!humanShareholderIds.isEmpty()) {
            allShareholderInfoMap.putAll(dao.batchQueryHumanOrCompanyInfo(humanShareholderIds));
        }
        if (!companyShareholderIds.isEmpty()) {
            allShareholderInfoMap.putAll(dao.batchQueryHumanOrCompanyInfo(companyShareholderIds));
        }
        List<Map<String, Object>> resultColumnMaps = allShareholders
                .values()
                .parallelStream()
                .filter(ratioPathCompanyDto -> {
                    // 补全股东信息
                    String shareholderId = ratioPathCompanyDto.getShareholderId();
                    Map<String, Object> shareholderInfoColumnMap = allShareholderInfoMap.getOrDefault(shareholderId, new HashMap<>());
                    String shareholderNameId = String.valueOf(shareholderInfoColumnMap.get("name_id"));
                    String shareholderMasterCompanyId = String.valueOf(shareholderInfoColumnMap.get("company_id"));
                    String shareholderLatestName = String.valueOf(shareholderInfoColumnMap.get("name"));
                    ratioPathCompanyDto.setShareholderNameId(shareholderNameId);
                    ratioPathCompanyDto.setShareholderMasterCompanyId(shareholderMasterCompanyId);
                    ratioPathCompanyDto.setShareholderName(shareholderLatestName);
                    // 脏数据过滤
                    return TycUtils.isUnsignedId(ratioPathCompanyDto.getCompanyId()) &&
                            TycUtils.isTycUniqueEntityId(ratioPathCompanyDto.getShareholderId()) &&
                            TycUtils.isUnsignedId(ratioPathCompanyDto.getShareholderNameId()) &&
                            TycUtils.isUnsignedId(ratioPathCompanyDto.getShareholderMasterCompanyId()) &&
                            TycUtils.isValidName(ratioPathCompanyDto.getCompanyName()) &&
                            TycUtils.isValidName(ratioPathCompanyDto.getShareholderName());
                })
                .map(RatioPathCompanyDto::toColumnMap).collect(Collectors.toList());
        // 没有合法股东
        if (resultColumnMaps.isEmpty()) {
            return Collections.singletonList(getSpecialRatioPathCompanyDto().toColumnMap());
        }
        return resultColumnMaps;
    }

    // 数据完整性, 自己投资自己
    private RatioPathCompanyDto getSpecialRatioPathCompanyDto() {
        BigDecimal ratio = BigDecimal.ONE;
        Edge newEdge = new Edge(ratio, true);
        Node newNode = new Node(companyId, companyName);
        Path newPath = Path.newPath(Path.newPath(new Node(companyId, companyName)), newEdge, newNode);
        RatioPathCompanyDto ratioPathCompanyDto = new RatioPathCompanyDto(companyId, companyName, companyIsListed, companyUscc, companyOrgType, companyEntityProperty, companyId, 1);
        ratioPathCompanyDto.setShareholderNameId(companyId);
        ratioPathCompanyDto.setShareholderMasterCompanyId(companyId);
        ratioPathCompanyDto.setShareholderName(companyName);
        ratioPathCompanyDto.getPaths().add(newPath);
        ratioPathCompanyDto.setTotalValidRatio(newPath.getValidRatio());
        ratioPathCompanyDto.setDirectShareholder(true);
        ratioPathCompanyDto.setDirectRatio(newPath.getValidRatio());
        ratioPathCompanyDto.setEnd(true);
        ratioPathCompanyDto.setShareholderLastAppearLevel(1);
        return ratioPathCompanyDto;
    }
}
