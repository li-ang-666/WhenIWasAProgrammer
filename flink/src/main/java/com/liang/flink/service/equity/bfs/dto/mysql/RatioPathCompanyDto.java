package com.liang.flink.service.equity.bfs.dto.mysql;

import cn.hutool.core.util.StrUtil;
import com.liang.common.util.JsonUtils;
import com.liang.flink.service.equity.bfs.dto.pojo.Edge;
import com.liang.flink.service.equity.bfs.dto.pojo.Node;
import com.liang.flink.service.equity.bfs.dto.pojo.Path;
import com.liang.flink.service.equity.bfs.dto.pojo.PathElement;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

@Data
@RequiredArgsConstructor
public class RatioPathCompanyDto {
    private static String[] USCC_WHITE_LIST = new String[]{"31", "91", "92", "93"};
    private static BigDecimal BIG_SHAREHOLDER = new BigDecimal("0.05");
    private static BigDecimal CONTROLLING_SHAREHOLDER = new BigDecimal("0.5");
    // 被投资公司基本属性
    private final String companyId;
    private final String companyName;
    private final boolean companyIsListed;
    private final String companyUscc;
    // 股东基本属性
    private final String shareholderId;
    private final String shareholderName;
    private final String shareholderNameId;
    private final String shareholderMasterCompanyId;
    // 路径明细 & 总比例
    private List<Path> paths = new ArrayList<>();
    private BigDecimal totalValidRatio = BigDecimal.ZERO;
    // 直接关系
    private boolean isDirectShareholder = false;
    private BigDecimal directRatio = BigDecimal.ZERO;
    // 是否穿透终点
    private boolean isEnd = false;

    public Map<String, Object> toColumnMap() {
        Map<String, Object> columnMap = new HashMap<>();
        // 公司
        columnMap.put("company_id", companyId);
        columnMap.put("company_name", companyName);
        //columnMap.put("company_is_listed", isListed);
        //columnMap.put("company_uscc", uscc);
        // 股东
        columnMap.put("shareholder_entity_type", shareholderId.length() == 17 ? "2" : "1");
        columnMap.put("shareholder_id", shareholderId);
        columnMap.put("shareholder_name", shareholderName);
        columnMap.put("shareholder_name_id", shareholderNameId);
        columnMap.put("shareholder_master_company_id", shareholderMasterCompanyId);
        // 投资
        columnMap.put("is_direct_shareholder", isDirectShareholder);
        columnMap.put("investment_ratio_direct", formatBigDecimal(directRatio, 6));
        columnMap.put("max_deliver", formatBigDecimal(getMaxDeliver(), 6));
        columnMap.put("investment_ratio_total", formatBigDecimal(totalValidRatio, 6));
        columnMap.put("equity_holding_path", JsonUtils.toString(allPaths2List()));
        columnMap.put("is_end", isEnd);
        // 标签
        boolean isWhiteUscc = StrUtil.startWithAny(companyUscc, USCC_WHITE_LIST);
        columnMap.put("is_big_shareholder", isWhiteUscc && isCompanyIsListed() && totalValidRatio.compareTo(directRatio) >= 0);
        columnMap.put("is_controlling_shareholder", isWhiteUscc && totalValidRatio.compareTo(CONTROLLING_SHAREHOLDER) >= 0);
        return columnMap;
    }

    /*
     * json数据结构:
     * [
     *   [ {head...}, {node...}, {edge...}, {node...}, {edge...}, {node...} ],
     *   [ {head...}, {node...}, {edge...}, {node...} ]
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
            put("total_percent", formatBigDecimal(path.getValidRatio().movePointRight(2), 4) + "%");
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
                    put("percent", formatBigDecimal(((Edge) element).getRatio().movePointRight(2), 4) + "%");
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
                        .orElse(BigDecimal.ZERO)
                )
                .max(BigDecimal::compareTo)
                .orElse(BigDecimal.ZERO);
    }

    public String formatBigDecimal(BigDecimal bigDecimal, int scale) {
        return bigDecimal.setScale(scale, RoundingMode.DOWN).toPlainString();
    }
}
