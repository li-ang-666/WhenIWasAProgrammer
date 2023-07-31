package org.tyc;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.tyc.entity.InvestmentRelation;
import org.tyc.entity.ShareHolderStopEnum;
import org.tyc.exception.BuildRootException;
import org.tyc.mybatis.mapper.company_legal_person.CompanyLegalPersonMapper;
import org.tyc.mybatis.mapper.share_holder_label.InvestmentRelationMapper;
import org.tyc.mybatis.mapper.share_holder_label.PersonnelEmploymentHistoryMapper;

import java.math.BigDecimal;
import java.util.*;

import static org.tyc.utils.InvestUtil.*;

@Slf4j
public class QueryAllShareHolderFromCompanyIdObj {
    /**
     * 入口参数
     * 只产出companyId 往上的所有的路径
     *
     * @param companyId 公司Id
     */
    public static Map<String, InvestmentRelation> queryAllShareHolderFromCompanyId(Long companyId,
                                                                                   int level,
                                                                                   InvestmentRelationMapper investmentRelationMapper,
                                                                                   PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper,
                                                                                   CompanyLegalPersonMapper companyLegalPersonMapper

    ) throws BuildRootException {

        // 路径结果存储
        Map<String, InvestmentRelation> resultMap = new HashMap<>();
        // 队列->用于实现广搜
        LinkedList<InvestmentRelation> queue = new LinkedList<>();
        // 被穿透的公司 入队，这也是起点
        InvestmentRelation root = buildRootInvestmentRelation(companyId, investmentRelationMapper);
        queue.addFirst(root);

        // 层数定义
        int curLevel = 1;
        // 广搜构造路径关系信息
        while (!queue.isEmpty() && curLevel <= level) {
            int size = queue.size();
            HashSet<String> investmentRelationVisited = new HashSet<>();
//            if (curLevel % 10 == 0) {
//                System.out.println("当前穿透公司：" + companyId + " 的 当前穿透层" + curLevel);
//            }
//            System.out.println("起点穿透公司：" + companyId + " 的 当前穿透层" + curLevel);
//            System.out.println("resultMap size" + resultMap.size());
            for (int i = 0; i < size; i++) {
                InvestmentRelation last = queue.removeLast();
                if (investmentRelationVisited.contains(last.getKey(companyId))) { // 层内不进行重复穿透
                    continue;
                }
                investmentRelationVisited.add(last.getKey(companyId));
                // 查询股东
                List<InvestmentRelation> investmentRelationList = investmentRelationMapper.getAllInvestmentRelationAsListByGid(getShareholderGraphIdFromInvestmentRelation(last.getShareholderEntityType(), last.getCompanyEntityInlink()));
                // 查询不出来的也需要重新置位
                if (investmentRelationList.size() == 0) {
                    if (resultMap.containsKey(last.getKey(companyId))) {
                        resultMap.get(last.getKey(companyId)).setEnd(1);
                    }
                } else {
                    // 遍历每个股东，构造或者更新路径
                    int shouldStopCnt = 0;
                    for (InvestmentRelation investmentRelation : investmentRelationList) {
                        InvestmentRelation newInvestmentRelation = buildNewInvestmentRelation(root, last, investmentRelation, resultMap, curLevel);
//                        System.out.println("当前层" + curLevel + "当前穿透股东为" + last.getShareholderNameId() + "得到新公司" + newInvestmentRelation.getShareholderNameId() + "比例" + newInvestmentRelation.getInvestmentRatio());
                        int shouldStopFlag = shouldStop(investmentRelation, curLevel, resultMap, newInvestmentRelation, companyId);
                        if (shouldStopFlag == ShareHolderStopEnum.STOP_NOT_ADD) { // 停止抛弃
                            shouldStopCnt++;
                        } else if (shouldStopFlag == ShareHolderStopEnum.STOP_ADD_PATH_ONLY) { // 停止需要加入路径
                            addRatioPathCompanyIntoResMap(resultMap, newInvestmentRelation, root.getCompanyIdInvested(), shouldStopFlag);
                            resultMap.get(newInvestmentRelation.getKey(companyId)).setEnd(1);
                        } else if (shouldStopFlag == ShareHolderStopEnum.STOP_ADD_PATH_RATIO) { // 停止但是需要加入路径和比例
                            addRatioPathCompanyIntoResMap(resultMap, newInvestmentRelation, root.getCompanyIdInvested(), shouldStopFlag);
                            resultMap.get(newInvestmentRelation.getKey(companyId)).setEnd(1);
                        } else { // 需要继续进行穿透
                            addRatioPathCompanyIntoResMap(resultMap, newInvestmentRelation, root.getCompanyIdInvested(), shouldStopFlag);
                            queue.addFirst(investmentRelation); // 添加节点继续穿透
                        }
                    }
                    if (shouldStopCnt == investmentRelationList.size() && shouldStopCnt != 0) {
                        if (resultMap.containsKey(last.getKey(companyId))) {
                            resultMap.get(last.getKey(companyId)).setEnd(1);
                        }
                    }
                    if (curLevel == 1) {
                        fixIsControllingShareholder(resultMap, root);
                    }
                }
            }
            curLevel++;
        }
        // 穿透完成之后，修改路径中的标志
        fixPositions(resultMap, root, personnelEmploymentHistoryMapper);
        fixIsController(resultMap, root);
        fixIsUltimate(resultMap, root, companyLegalPersonMapper, personnelEmploymentHistoryMapper);
        f(resultMap);
        return resultMap;
    }

    // 1.1 新增规则, 如果某人是实际控制人, 路径上总股权超过50%的company,也标记为控制人
    private static void f(Map<String, InvestmentRelation> resultMap) {
        final BigDecimal decimal = new BigDecimal("0.5");
        for (Map.Entry<String, InvestmentRelation> entry : resultMap.entrySet()) {
            String companyIdAndShareholderTypeAndShareholderId = entry.getKey();
            String companyId = companyIdAndShareholderTypeAndShareholderId.replaceAll("#\\d#.*", "");
            //担心有脏数据
            if (!StringUtils.isNumeric(companyId)) {
                continue;
            }
            InvestmentRelation ratioPathCompany = entry.getValue();
            if (ratioPathCompany.getIsController() != 1) {
                continue;
            }
            LinkedList<LinkedList<InvestmentRelation>> struct = ratioPathCompany.getPath().getStruct();
            //遍历每条路径
            for (LinkedList<InvestmentRelation> route : struct) {
                //遍历每条路径的每个节点
                for (InvestmentRelation node : route) {
                    String inLink = node.getCompanyEntityInlink();
                    //如果是公司
                    if (inLink.matches(".*?:\\d+:company")) {
                        String[] splitInLink = inLink.split(":");
                        String shareholderId = splitInLink[splitInLink.length - 2];
                        String resultMapKey = companyId + "#1#" + shareholderId;
                        InvestmentRelation investmentRelation = resultMap.get(resultMapKey);
                        //如果股权大于50%
                        if (investmentRelation != null && investmentRelation.getInvestmentRatio().compareTo(decimal) >= 0) {
                            //如果不是实控人
                            if (investmentRelation.getIsController() != 1) {
                                //补充实控权
                                investmentRelation.setIsController(2);
                            }
                        }
                    }
                }
            }
        }
    }
}
