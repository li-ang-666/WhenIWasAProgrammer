package org.tyc;


import org.tyc.entity.DigitEnum;
import org.tyc.entity.EntityTypeEnum;
import org.tyc.entity.InvestmentRelation;
import org.tyc.entity.ShareHolderStopEnum;
import org.tyc.exception.BuildRootException;
import org.tyc.mybatis.mapper.company_legal_person.CompanyLegalPersonMapper;
import org.tyc.mybatis.mapper.share_holder_label.InvestmentRelationMapper;
import org.tyc.mybatis.mapper.share_holder_label.NoShareholderCompanyInfoMapper;
import org.tyc.mybatis.mapper.share_holder_label.PersonnelEmploymentHistoryMapper;

import java.util.*;

import static org.tyc.utils.InvestUtil.*;


public class QueryNoAllShareHolderFromCompanyIdObj {

    /**
     * 构造root节点
     *
     * @param companyId companyId
     * @param mapper    mapper
     */
    public static InvestmentRelation buildRootInvestmentRelationNo(Long companyId, NoShareholderCompanyInfoMapper mapper) throws BuildRootException {
        InvestmentRelation aInvestmentRelationAsListByGid = null;
        try {
            aInvestmentRelationAsListByGid = mapper.getAInvestmentRelationAsListByGid(companyId);
            // 设置股东侧部分变量用作穿透
            aInvestmentRelationAsListByGid.setInvestmentRatio(DigitEnum.RATION100);
            aInvestmentRelationAsListByGid.setCompanyEntityInlink(":".concat(aInvestmentRelationAsListByGid.getCompanyIdInvested().toString()).concat(":company"));
            aInvestmentRelationAsListByGid.setShareholderEntityType(EntityTypeEnum.COMPANY);
        } catch (Exception e) {
            throw new BuildRootException(companyId);
        }
        return aInvestmentRelationAsListByGid;
    }

    /**
     * 入口参数
     * 只产出companyId 往上的所有的路径
     *
     * @param companyId 公司Id
     */
    public static Map<String, InvestmentRelation> queryNoAllShareHolderFromCompanyId(Long companyId,
                                                                                     int level,
                                                                                     NoShareholderCompanyInfoMapper noShareholderCompanyInfoMapper,
                                                                                     PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper,
                                                                                     CompanyLegalPersonMapper companyLegalPersonMapper,
                                                                                     InvestmentRelationMapper investmentRelationMapper

    ) throws BuildRootException {
        // 路径结果存储
        Map<String, InvestmentRelation> resultMap = new HashMap<>();

        // 判读是否在投资关系中,如果查出来有则证明是有股东的公司，不进行下面的操作
        Long haveAnyShareholders = investmentRelationMapper.getHaveAnyShareholders(companyId);
        if ((haveAnyShareholders != null) && haveAnyShareholders > 0) {
//            System.out.println("有投资关系");
            return resultMap;
        }


        // 队列->用于实现广搜
        LinkedList<InvestmentRelation> queue = new LinkedList<>();
        // 被穿透的公司 入队，这也是起点
        InvestmentRelation root = buildRootInvestmentRelationNo(companyId, noShareholderCompanyInfoMapper);
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
                List<InvestmentRelation> investmentRelationList = new ArrayList<>();
                // 查询不出来的也需要重新置位
                if (resultMap.containsKey(last.getKey(companyId))) {
                    resultMap.get(last.getKey(companyId)).setEnd(1);
                }
                if (investmentRelationList.size() == 0) {

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
        return resultMap;
    }

}
