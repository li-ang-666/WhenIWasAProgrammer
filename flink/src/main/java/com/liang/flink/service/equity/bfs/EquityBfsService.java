package com.liang.flink.service.equity.bfs;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;
import com.liang.flink.service.equity.bfs.dto.Operation;
import com.liang.flink.service.equity.bfs.dto.mysql.CompanyEquityRelationDetailsDto;
import com.liang.flink.service.equity.bfs.dto.mysql.RatioPathCompanyDto;
import com.liang.flink.service.equity.bfs.dto.pojo.Edge;
import com.liang.flink.service.equity.bfs.dto.pojo.Node;
import com.liang.flink.service.equity.bfs.dto.pojo.Path;
import com.liang.flink.service.equity.bfs.dto.pojo.PathElement;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

import static com.liang.flink.service.equity.bfs.dto.Operation.*;

@Slf4j
public class EquityBfsService {
    private static final BigDecimal THRESHOLD = new BigDecimal("0.000001");
    private static final BigDecimal ONE_HUNDRED = new BigDecimal("100");
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
        //debugShareholderMap();
        Map<String, Object> columnMap = ratioPathCompanyDto2ColumnMap(allShareholders.get("V0M9EM200ND6FPNUP"));
        columnMap.forEach((k, v) -> System.out.println(k + " -> " + v));
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
        allShareholders.compute(shareholderId, (k, v) -> {
            RatioPathCompanyDto ratioPathCompanyDto;
            if (v != null) {
                ratioPathCompanyDto = v;
            } else {
                // 自然人股东
                if (shareholderId.length() == 17) {
                    String shareholderType = "2";
                    Map<String, Object> shareholderInfoColumnMap = dao.queryHumanInfo(shareholderId);
                    String shareholderNameId = String.valueOf(shareholderInfoColumnMap.get("human_name_id"));
                    String shareholderMasterCompanyId = String.valueOf(shareholderInfoColumnMap.get("master_company_id"));
                    ratioPathCompanyDto = new RatioPathCompanyDto(companyId, companyName, shareholderType, shareholderId, shareholderName, shareholderNameId, shareholderMasterCompanyId);
                }
                // 公司股东
                else {
                    String shareholderType = "1";
                    ratioPathCompanyDto = new RatioPathCompanyDto(companyId, companyName, shareholderType, shareholderId, shareholderName, shareholderId, shareholderId);
                }
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

    private Map<String, Object> ratioPathCompanyDto2ColumnMap(RatioPathCompanyDto ratioPathCompanyDto) {
        List<List<Map<String, Object>>> pathList = new ArrayList<>();
        // 每条path
        for (Path path : ratioPathCompanyDto.getPaths()) {
            List<Map<String, Object>> pathElementList = new ArrayList<>();
            // head
            pathElementList.add(path2Head(path));
            // 每个元素(点、边)
            for (PathElement element : path.getElements()) {
                pathElementList.add(1, element2Map(element, ratioPathCompanyDto));
            }
            // save
            pathList.add(pathElementList);
        }
        Map<String, Object> columnMap = new HashMap<>();
        columnMap.put("equity_holding_path", JsonUtils.toString(pathList));
        columnMap.put("company_id", ratioPathCompanyDto.getCompanyId());
        columnMap.put("company_name", ratioPathCompanyDto.getCompanyName());
        columnMap.put("shareholder_id", ratioPathCompanyDto.getShareholderId());
        columnMap.put("shareholder_name", ratioPathCompanyDto.getShareholderName());
        columnMap.put("shareholder_name_id", ratioPathCompanyDto.getShareholderNameId());
        columnMap.put("investment_ratio_total", ratioPathCompanyDto.getTotalValidRatio().stripTrailingZeros().toPlainString());
        columnMap.put("is_deleted", 0);
        return columnMap;
    }

    private Map<String, Object> path2Head(Path path) {
        return new LinkedHashMap<String, Object>() {{
            put("is_red", "0");
            put("path_usage", "1");
            put("total_percent", path.getValidRatio().multiply(ONE_HUNDRED).stripTrailingZeros().toPlainString() + "%");
            put("type", "summary");
        }};
    }

    private Map<String, Object> element2Map(PathElement element, RatioPathCompanyDto ratioPathCompanyDto) {
        Map<String, Object> pathElementInfoMap = new LinkedHashMap<>();
        // 点
        if (element instanceof Node) {
            String shareholderId = ((Node) element).getId();
            // 自然人
            if (shareholderId.length() == 17) {
                pathElementInfoMap.put("type", "human");
                pathElementInfoMap.put("cid", ratioPathCompanyDto.getShareholderMasterCompanyId());
                pathElementInfoMap.put("hid", ratioPathCompanyDto.getShareholderNameId());
                pathElementInfoMap.put("pid", shareholderId);
                pathElementInfoMap.put("name", ((Node) element).getName());
            }
            // 公司
            else {
                pathElementInfoMap.put("type", "company");
                pathElementInfoMap.put("cid", shareholderId);
                pathElementInfoMap.put("name", ((Node) element).getName());
            }
        }
        // 边
        else {
            pathElementInfoMap.put("type_count", "1");
            pathElementInfoMap.put("edges", new ArrayList<Map<String, Object>>() {{
                add(new LinkedHashMap<String, Object>() {{
                    put("type", "INVEST");
                    put("percent", ((Edge) element).getRatio().multiply(ONE_HUNDRED).stripTrailingZeros().toPlainString() + "%");
                    put("source", "80");
                }});
            }});
        }
        return pathElementInfoMap;
    }
}
