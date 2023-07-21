package org.tyc.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.tyc.entity.CompanyLink;
import org.tyc.entity.HumanLink;
import org.tyc.entity.InvestmentRelation;
import org.tyc.entity.Path;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.tyc.utils.InvestUtil.getCompanyLink;
import static org.tyc.utils.InvestUtil.getHumanLink;

/**
 * @author: 邵静虎
 * @Time: 2023/5/13 13:48
 */
public class PathFormatter {
    static DecimalFormat format = new DecimalFormat("0.00");

    private static String format(BigDecimal bigDecimal) {
        return bigDecimal
                .setScale(2, RoundingMode.DOWN)
                .toPlainString();
    }

    // 将InvestmentRelation转换为Path路径
    public static JSONArray formatInvestmentRelationToPath(InvestmentRelation investmentRelation) {
        JSONArray pathResult = new JSONArray();
        // 每条路径一个数组元素
        Path path = investmentRelation.getPath();
        if (path == null) {
            return pathResult;
        }
        for (LinkedList<InvestmentRelation> singlePath : path.getStruct()) {
            // 单条路径
            JSONArray singlePathJSON = new JSONArray();

            // 这条路径的一些信息
            JSONObject pathInfoJSON = new JSONObject();
            pathInfoJSON.put("is_red", 0);
            pathInfoJSON.put("path_usage", 1);
            //pathInfoJSON.put("total_percent", "" + format.format(singlePathTotalPercent(singlePath).multiply(new BigDecimal("100"))) + "%");
            pathInfoJSON.put("total_percent", format(singlePathTotalPercent(singlePath).multiply(new BigDecimal("100"))) + "%");
            pathInfoJSON.put("type", "summary");
            singlePathJSON.add(pathInfoJSON);

            // 注意从后往前遍历
            int i = singlePath.size();
            while (--i >= 0) {
                InvestmentRelation elem = singlePath.get(i);
                if (i == singlePath.size() - 1) {
                    // 股东加进去
                    singlePathJSON.add(transformInLinkToNode(elem.getCompanyEntityInlink()));
                }
                // 加边的关系
                JSONObject edgesInfo = new JSONObject();
                edgesInfo.put("type_count", 1);
                JSONArray edges = new JSONArray();
                JSONObject edge = new JSONObject();

                edge.put("source", 80);
                edge.put("type", "INVEST");
                //edge.put("percent", "" + format.format(elem.getInvestmentRatio().multiply(new BigDecimal("100"))) + "%");
                edge.put("percent", format(elem.getInvestmentRatio().multiply(new BigDecimal("100"))) + "%");

                edges.add(edge);
                edgesInfo.put("edges", edges);
                singlePathJSON.add(edgesInfo);
                // 被投资人加进去

                singlePathJSON.add(transformInLinkToNode(elem.getCompanyNameInvested() + ":" + elem.getCompanyIdInvested() + ":company"));
            }
            pathResult.add(singlePathJSON);
        }
        return pathResult;
    }

    // 计算一条路径的总投资投资比例
    public static BigDecimal allPathTotalPercent(InvestmentRelation ir) {
        BigDecimal totalPercent = new BigDecimal(0L);
        if (ir == null || ir.getPath() == null || ir.getPath().getStruct() == null) {
            return totalPercent;
        }
        for (LinkedList<InvestmentRelation> investmentRelations : ir.getPath().getStruct()) {
            totalPercent = totalPercent.add(singlePathTotalPercent(investmentRelations));
        }
        return totalPercent;
    }

    // 计算一条路径的总投资投资比例
    public static BigDecimal singlePathTotalPercent(List<InvestmentRelation> singlePath) {
        BigDecimal totalPercent = new BigDecimal(1L);
        for (InvestmentRelation investmentRelation : singlePath) {
            totalPercent = investmentRelation.getInvestmentRatio().multiply(totalPercent);
        }
        return totalPercent;
    }

    static JSONObject transformInLinkToNode(String inLink) {
        JSONObject jsonObject = new JSONObject();
        if (inLink == null) {
            return jsonObject;
        }
        String[] strings = inLink.split(":");
        if (strings.length < 3) {
            return jsonObject;
        } else if (Objects.equals(strings[strings.length - 1], "human")) {
            HumanLink humanLink = getHumanLink(inLink);
            // 人
            jsonObject.put("hid", humanLink.getHumanNameId());
            jsonObject.put("name", humanLink.getHumanName());
            jsonObject.put("type", "human");
            jsonObject.put("cid", humanLink.getCompanyId());
        } else if (Objects.equals(strings[strings.length - 1], "company")) {
            CompanyLink companyLink = getCompanyLink(inLink);
            // 公司
            jsonObject.put("name", companyLink.getCompanyName());
            jsonObject.put("type", "company");
            jsonObject.put("cid", companyLink.getCompanyId());
        }
        return jsonObject;
    }
}
