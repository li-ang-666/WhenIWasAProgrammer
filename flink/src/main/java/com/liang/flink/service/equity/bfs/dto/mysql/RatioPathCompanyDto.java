package com.liang.flink.service.equity.bfs.dto.mysql;

import com.liang.common.util.JsonUtils;
import com.liang.flink.service.equity.bfs.dto.pojo.Edge;
import com.liang.flink.service.equity.bfs.dto.pojo.Node;
import com.liang.flink.service.equity.bfs.dto.pojo.Path;
import com.liang.flink.service.equity.bfs.dto.pojo.PathElement;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.*;

import static java.math.BigDecimal.ZERO;

@Data
@RequiredArgsConstructor
public class RatioPathCompanyDto {
    private static final BigDecimal ONE_HUNDRED = new BigDecimal("100");

    // 被投资公司基本属性
    private final String companyId;
    private final String companyName;

    // 股东基本属性
    private final String shareholderType;
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private final String shareholderMasterCompanyId;

    // 路径明细 & 总比例
    private List<Path> paths = new ArrayList<>();
    private BigDecimal totalValidRatio = ZERO;

    // 直接关系
    private boolean isDirectShareholder = false;
    private BigDecimal directRatio = ZERO;

    // 是否穿透终点
    private boolean isEnd = false;

    public Map<String, Object> toColumnMap() {
        Map<String, Object> columnMap = new HashMap<>();
        // 公司
        columnMap.put("company_id", companyId);
        columnMap.put("company_name", companyName);
        // 股东
        columnMap.put("shareholder_entity_type", shareholderType);
        columnMap.put("shareholder_id", shareholderId);
        columnMap.put("shareholder_name", shareholderName);
        columnMap.put("shareholder_name_id", shareholderNameId);
        columnMap.put("shareholder_master_company_id", shareholderMasterCompanyId);
        // 投资
        columnMap.put("is_direct_shareholder", isDirectShareholder);
        columnMap.put("investment_ratio_direct", directRatio.stripTrailingZeros().toPlainString());
        columnMap.put("max_deliver", getMaxDeliver());
        columnMap.put("investment_ratio_total", totalValidRatio.stripTrailingZeros().toPlainString());
        columnMap.put("equity_holding_path", JsonUtils.toString(allPaths2List()));
        return columnMap;
    }

    /*
     * json数据结构:
     * [
     *   [ {head...} {node...} {edge...} {node...} {edge...} {node...} ],
     *   [ {head...} {node...} {edge...} {node...} ]
     * ]
     */
    private List<List<Map<String, Object>>> allPaths2List() {
        List<List<Map<String, Object>>> pathList = new ArrayList<>();
        // 每条path
        for (Path path : paths) {
            List<Map<String, Object>> pathElementList = new ArrayList<>();
            // head
            pathElementList.add(singlePath2HeadMap(path));
            // 每个元素(点 or 边)
            for (PathElement element : path.getElements()) {
                pathElementList.add(1, singleElement2ElementMap(element));
            }
            // save
            pathList.add(pathElementList);
        }
        return pathList;
    }

    private Map<String, Object> singlePath2HeadMap(Path path) {
        return new LinkedHashMap<String, Object>() {{
            put("is_red", "0");
            put("total_percent", path.getValidRatio().multiply(ONE_HUNDRED).stripTrailingZeros().toPlainString() + "%");
            put("path_usage", "1");
            put("type", "summary");
        }};
    }

    private Map<String, Object> singleElement2ElementMap(PathElement element) {
        Map<String, Object> pathElementInfoMap = new LinkedHashMap<>();
        // 点
        if (element instanceof Node) {
            String shareholderId = ((Node) element).getId();
            // 自然人
            if (shareholderId.length() == 17) {
                pathElementInfoMap.put("type", "human");
                pathElementInfoMap.put("hid", shareholderNameId);
                pathElementInfoMap.put("cid", shareholderMasterCompanyId);
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

    /*
     * path不可能为empty
     * 所以直接optional.orElse(ZERO)
     */
    public BigDecimal getMaxDeliver() {
        return paths.parallelStream()
                .map(singlePath -> singlePath.getElements().parallelStream()
                        .filter(element -> element instanceof Edge)
                        .map(element -> ((Edge) element).getRatio())
                        .min(BigDecimal::compareTo)
                        .orElse(ZERO)
                )
                .max(BigDecimal::compareTo)
                .orElse(ZERO);
    }
}
