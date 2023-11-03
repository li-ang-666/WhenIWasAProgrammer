package org.tyc.utils;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.SqlUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tyc.entity.*;
import org.tyc.exception.BuildRootException;
import org.tyc.mybatis.mapper.company_legal_person.CompanyLegalPersonMapper;
import org.tyc.mybatis.mapper.share_holder_label.InvestmentRelationMapper;
import org.tyc.mybatis.mapper.share_holder_label.PersonnelEmploymentHistoryMapper;

import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Pattern;

import static org.tyc.entity.TwoCreditCodeEnum.ForUltimater;

public class InvestUtil {
    // 不包含的企业类型
    public static final Set<String> NotContainForeignCompanyType = new HashSet<>(Arrays.asList("外国法人独资", "外国自然人独资", "台港澳与外国投资者合资", "新闻", "旅游", "外国非法人"));
    private static final Logger LOGGER = LoggerFactory.getLogger(InvestUtil.class);

    /**
     * 构造路径结果
     * 直接股东中的大股东逻辑在这
     *
     * @param root               被投资公司
     * @param investmentRelation 投资公司
     * @param curLevel           层级
     * @return 构造的路径
     */
    public static InvestmentRelation buildNewInvestmentRelation(InvestmentRelation root,  // 固定的根节点
                                                                InvestmentRelation investmentRelation,  // 当前访问的节点
                                                                InvestmentRelation investmentRelationShareHolder, // 从数据库查出的当前节点的股东节点
                                                                Map<String, InvestmentRelation> resultMap, // 根节点已经存在的路径，用于后续拼接path 和 percent
                                                                int curLevel) {
        InvestmentRelation newInvestmentRelation = new InvestmentRelation();

        newInvestmentRelation.setId(null);
        newInvestmentRelation.setShareholderEntityType(investmentRelationShareHolder.getShareholderEntityType());
        newInvestmentRelation.setCompanyEntityInlink(investmentRelationShareHolder.getCompanyEntityInlink());
        newInvestmentRelation.setCompanyType(investmentRelationShareHolder.getCompanyType());
        newInvestmentRelation.setShareholderNameId(investmentRelationShareHolder.getShareholderNameId());
        newInvestmentRelation.setIsListedCompany(investmentRelationShareHolder.getIsListedCompany());
        newInvestmentRelation.setIsForeignBranches(investmentRelationShareHolder.getIsForeignBranches());
        newInvestmentRelation.setListedCompanyActualController(investmentRelationShareHolder.getListedCompanyActualController());
        newInvestmentRelation.setIsPartnershipCompany(investmentRelationShareHolder.getIsPartnershipCompany());
        newInvestmentRelation.setLegalRepInlinks(investmentRelationShareHolder.getLegalRepInlinks());
        newInvestmentRelation.setCompanyUsccPrefixCodeTwo(investmentRelationShareHolder.getCompanyUsccPrefixCodeTwo());

        newInvestmentRelation.setCompanyIdInvested(root.getCompanyIdInvested());
        newInvestmentRelation.setCompanyTypeInvested(root.getCompanyTypeInvested());
        newInvestmentRelation.setIsListedCompanyInvested(root.getIsListedCompanyInvested());
        newInvestmentRelation.setIsForeignBranchesInvested(root.getIsForeignBranchesInvested());
        newInvestmentRelation.setListedCompanyActualControllerInvested(root.getListedCompanyActualControllerInvested());
        newInvestmentRelation.setIsPartnershipCompany(root.getIsPartnershipCompany());
        newInvestmentRelation.setLegalRepInlinksInvested(root.getLegalRepInlinksInvested());
        newInvestmentRelation.setCompanyUsccPrefixCodeTwoInvested(root.getCompanyUsccPrefixCodeTwoInvested());

        // 这里只存了相邻关系层的职位
        newInvestmentRelation.setShareholderCompanyPositionListClean(curLevel == 1 ? investmentRelationShareHolder.getShareholderCompanyPositionListClean() : null);
        newInvestmentRelation.setInvestmentRatio(getPercentFromResultMap(investmentRelation, investmentRelationShareHolder, resultMap, root.getCompanyIdInvested(), curLevel));
        newInvestmentRelation.setPath(buildPathFromResultMap(investmentRelation, investmentRelationShareHolder, resultMap, root.getCompanyIdInvested(), curLevel));
        newInvestmentRelation.setCreateTime(investmentRelationShareHolder.getCreateTime());
        newInvestmentRelation.setUpdateTime(investmentRelationShareHolder.getUpdateTime());

        newInvestmentRelation.setIsController(0);
        newInvestmentRelation.setIsControllingShareholder(0);
        newInvestmentRelation.setIsUltimate(0);
        newInvestmentRelation.setEnd(0);
        /*
         * 大股东的逻辑：
         * 因为只涉及到两个实体的关系不涉及层穿透的完整性
         * 故在此判定是否是大股东
         */
        if (curLevel == 1 && TwoCreditCodeEnum.ForBigShareHolder.contains(newInvestmentRelation.getCompanyUsccPrefixCodeTwoInvested()) && newInvestmentRelation.ifInvestmentRatioGreaterEqualThan(DigitEnum.RATION005)) {
            newInvestmentRelation.setIsBigShareholder(1);
        } else {
            newInvestmentRelation.setIsBigShareholder(0);
        }
        return newInvestmentRelation;
    }

    /**
     * 构造路径
     *
     * @param investmentRelation            当前节点
     * @param investmentRelationShareHolder 股东节点
     * @param resultMap                     map
     * @param edCompanyId                   root.companyIdInvested
     * @param curLevel                      层级
     */
    private static Path buildPathFromResultMap(InvestmentRelation investmentRelation, InvestmentRelation investmentRelationShareHolder, Map<String, InvestmentRelation> resultMap, Long edCompanyId, int curLevel) {
        if (resultMap.containsKey(investmentRelation.getKey(edCompanyId))) { // 存在前驱
            InvestmentRelation preInvestmentRelation = resultMap.get(investmentRelation.getKey(edCompanyId));
            Path investmentRelationPath = preInvestmentRelation.getPath();
            Path newPath = new Path();
            if (investmentRelationPath == null) {
                newPath.addNewPathLink(investmentRelationShareHolder);
            } else {
                LinkedList<LinkedList<InvestmentRelation>> struct = new LinkedList<LinkedList<InvestmentRelation>>();
                for (LinkedList<InvestmentRelation> linkedList : investmentRelationPath.clone().struct) {
                    if (linkedList.size() == curLevel - 1) { //
                        struct.add(linkedList);
                    }
                }
                newPath.setStruct(struct);
                newPath.appendAtEachPath(investmentRelationShareHolder);
            }
            return newPath;
        } else { // 不存在前驱
            Path newPath = new Path();
            newPath.addNewPathLink(investmentRelationShareHolder);
            return newPath;
        }
    }

    /**
     * 获取持股比例
     *
     * @param investmentRelation            当前节点
     * @param investmentRelationShareHolder 当前节点股东
     * @param resultMap                     map
     * @param edCompanyId                   root.companyIdInvested
     * @param curLevel                      层级
     */
    public static BigDecimal getPercentFromResultMap(InvestmentRelation investmentRelation, InvestmentRelation investmentRelationShareHolder, Map<String, InvestmentRelation> resultMap, Long edCompanyId, int curLevel) {
        if (resultMap.containsKey(investmentRelation.getKey(edCompanyId))) {
            InvestmentRelation preInvestmentRelation = resultMap.get(investmentRelation.getKey(edCompanyId));
            BigDecimal prePercent = getAllSumInCurrentLevel(preInvestmentRelation, curLevel);
            BigDecimal investmentRatio = investmentRelationShareHolder.getInvestmentRatio();
            if (prePercent.multiply(investmentRatio).compareTo(DigitEnum.RATION100) > 0) {
                return DigitEnum.RATION100;
            } else {
                return prePercent.multiply(investmentRatio);
            }
        } else {
            if (investmentRelationShareHolder.getInvestmentRatio().compareTo(DigitEnum.RATION100) > 0) {
                return DigitEnum.RATION100;
            } else {
                return investmentRelationShareHolder.getInvestmentRatio();
            }

        }
    }

    /**
     * 同层内的要相加
     *
     * @param investmentRelation 被投资
     * @param curLevel           层
     */
    private static BigDecimal getAllSumInCurrentLevel(InvestmentRelation investmentRelation, int curLevel) {
        BigDecimal res = new BigDecimal("0.00");
        for (LinkedList<InvestmentRelation> linkedList : investmentRelation.getPath().struct) {
            BigDecimal tmp = new BigDecimal("0.00");
            if (linkedList.size() == curLevel - 1) {
                for (InvestmentRelation relation : linkedList) {
                    if (tmp.compareTo(new BigDecimal("0.00")) == 0) {
                        tmp = relation.getInvestmentRatio();
                    } else {
                        tmp = relation.getInvestmentRatio().multiply(tmp);
                    }
                }
            }
            res = res.add(tmp);
        }
        return res;
    }

    /**
     * 获取股东的gid
     *
     * @param shareholderType     股东类型
     * @param companyEntityInLink 股东的内链
     */
    public static Long getShareholderGraphIdFromInvestmentRelation(Integer shareholderType, String companyEntityInLink) {
        if (Objects.equals(shareholderType, EntityTypeEnum.PERSON)) { // 崔珊珊:1905070872-22822:U004AMH000T0444SH:human
            return getHumanLink(companyEntityInLink).getHumanNameId();
        } else if (Objects.equals(shareholderType, EntityTypeEnum.COMPANY)) { // 上海同兴袜厂:608822228:company
            return getCompanyLink(companyEntityInLink).getCompanyId();
        } else return 0L;
    }

    /**
     * 解析老板的内链
     *
     * @param input
     * @return
     */
    public static HumanLink getHumanLink(String input) {
        HumanLink humanLink = new HumanLink();
        String[] split = input.split(":");
        if (Objects.equals(split[split.length - 1], "human")) {
            if (split.length == 4) {
                humanLink.setHumanId(split[2]);
                String[] splitHuman = split[1].split("-");
                if (splitHuman.length == 2) {
                    humanLink.setCompanyId(CastString2Long(splitHuman[1], 0L));
                    humanLink.setHumanNameId(CastString2Long(splitHuman[0], 0L));
                }
                humanLink.setHumanName(split[0]);
            } else if (split.length > 4) {
                humanLink.setHumanId(split[split.length - 2]);
                String[] splitHuman = split[split.length - 3].split("-");
                if (splitHuman.length == 2) {
                    humanLink.setCompanyId(CastString2Long(splitHuman[1], 0L));
                    humanLink.setHumanNameId(CastString2Long(splitHuman[0], 0L));
                }
                StringBuilder stringBuilder = new StringBuilder(2001);
                for (int i = 0; i < split.length - 3; i++) {
                    stringBuilder.append(split[i]);
                    if (i != split.length - 3 - 1) {
                        stringBuilder.append(":");
                    }
                }
                humanLink.setHumanName(stringBuilder.toString());
            }
        }

        return humanLink;


    }

    /**
     * 解析公司的内链.
     *
     * @param input
     * @return
     */
    public static CompanyLink getCompanyLink(String input) {
        CompanyLink companyLink = new CompanyLink();

        String[] split = input.split(":");
        if (Objects.equals(split[split.length - 1], "company")) {
            if (split.length == 3) {
                companyLink.setCompanyId(CastString2Long(split[1], 0L));
                companyLink.setCompanyName(split[0]);
            } else if (split.length > 3) {
                companyLink.setCompanyId(CastString2Long(split[split.length - 2], 0L));
                StringBuilder stringBuilder = new StringBuilder(200);
                for (int i = 0; i < split.length - 2; i++) {

                    stringBuilder.append(split[i]);
                    if (i != split.length - 2 - 1) {
                        stringBuilder.append(":");
                    }
                }
                companyLink.setCompanyName(stringBuilder.toString());
            }
        }
        return companyLink;

    }

    private static Long CastString2Long(String str, Long defaultValue) {
        try {
            return Long.valueOf(str);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 获取股东id 如果是人的话pid 公司的话返回的是gid
     *
     * @param shareholderType     股东类型
     * @param companyEntityInLink 股东的内链信息
     */
    public static String getShareholderId(Integer shareholderType, String companyEntityInLink) {

        if (Objects.equals(shareholderType, EntityTypeEnum.PERSON)) {
            return getHumanLink(companyEntityInLink).getHumanId();
        } else if (Objects.equals(shareholderType, EntityTypeEnum.COMPANY)) {
            return String.valueOf(getCompanyLink(companyEntityInLink).getCompanyId());
        }

        return "";
    }

    /**
     * 找到集合中的控股股东并打上标签.
     */
    public static void fixIsControllingShareholder(Map<String, InvestmentRelation> resultMap, InvestmentRelation root) {
        /**
         * 过滤起点公司的统一社会信用代码
         */
        String edCompanyTwoCreditCode = root.getCompanyUsccPrefixCodeTwoInvested();
        if (!TwoCreditCodeEnum.ForBigControllingShareHolder.contains(edCompanyTwoCreditCode)) {
            return;
        }


        // 1【规则】直接持股比例>50%的,为控股股东。
        if (resultMap.size() >= 1) {
            List<InvestmentRelation> resultList = new ArrayList<>(resultMap.size());
            resultList.addAll(resultMap.values());
            Collections.sort(resultList, (o1, o2) -> o2.getInvestmentRatio().compareTo(o1.getInvestmentRatio()));


            if (resultList.size() == 1) {
                InvestmentRelation entryFirst = resultList.get(0);
                if (entryFirst.getInvestmentRatio().compareTo(DigitEnum.RATION050) >= 0) {
                    entryFirst.isControllingShareholder = 1;
                    return;
                }

            } else {
                InvestmentRelation entryFirst = resultList.get(0);
                InvestmentRelation entrySecond = resultList.get(1);
                if (entryFirst.getInvestmentRatio().compareTo(DigitEnum.RATION050) >= 0 && entrySecond.getInvestmentRatio().compareTo(DigitEnum.RATION050) < 0) {
                    entryFirst.isControllingShareholder = 1;
                    return;
                }
            }
        }


        // 2【规则】直接持股比例=50%且直接股东数量=2的,根据股东身份属性及任职岗位进行判断
        if (resultMap.size() == 2) {
            List<String> keyList = new ArrayList<>(resultMap.keySet());
            InvestmentRelation entryFirst = resultMap.getOrDefault(keyList.get(0), null);
            InvestmentRelation entrySecond = resultMap.getOrDefault(keyList.get(1), null);

            // 直接持股比例=50%且直接股东数量=2的,根据股东身份属属性及任职岗位进行判断:
            if (entryFirst != null && entrySecond != null && entryFirst.getInvestmentRatio().compareTo(DigitEnum.RATION050) == 0 && entrySecond.getInvestmentRatio().compareTo(DigitEnum.RATION050) == 0) {
                if (entryFirst.ifNotInvestorIsPerson() && entrySecond.ifNotInvestorIsPerson()) {
                    // a 股东身份属性:两个都是非自然人的,则直接为控股股东。(此均场景下控股股东有两个)
                    entryFirst.isControllingShareholder = 1;
                    entrySecond.isControllingShareholder = 1;
                } else if (entryFirst.ifInvestorIsPerson() && entrySecond.ifNotInvestorIsPerson()) {
                    // 股东身份属性:直接持股股东中至少有一个是自然人
                    // 一个自然人和一个非自然人
                    // 非自然人,则直接为控股股东
                    entrySecond.isControllingShareholder = 1;
                    if (entryFirst.ifCompanyItemPositions("董事长", "执行董事")) {
                        entryFirst.isControllingShareholder = 1;
                    }
                } else if (entrySecond.ifInvestorIsPerson() && entryFirst.ifNotInvestorIsPerson()) {
                    // 股东身份属性:直接持股股东中至少有一个是自然人
                    // 一个自然人和一个非自然人
                    // 非自然人,则直接为控股股东
                    entryFirst.isControllingShareholder = 1;
                    if (entrySecond.ifCompanyItemPositions("董事长", "执行董事")) {
                        entrySecond.isControllingShareholder = 1;
                    }
                } else if (entrySecond.ifInvestorIsPerson() && entryFirst.ifInvestorIsPerson()) {
                    // 两个都是自然人
                    if (entryFirst.ifCompanyItemPositions("董事长", "执行董事") || entrySecond.ifCompanyItemPositions("董事长", "执行董事")) {

                        // 保证董事长优先，并且结果只有一个人
                        if (entryFirst.ifCompanyItemPositions("董事长")) {
                            entryFirst.isControllingShareholder = 1;
                        } else if (entrySecond.ifCompanyItemPositions("董事长")) {
                            entrySecond.isControllingShareholder = 1;
                        } else if (entryFirst.ifCompanyItemPositions("执行董事")) {
                            entryFirst.isControllingShareholder = 1;
                        } else if (entrySecond.ifCompanyItemPositions("执行董事")) {
                            entrySecond.isControllingShareholder = 1;
                        } else {
                            entryFirst.isControllingShareholder = 1;
                        }
                    }
                }
                return;
            }
        }

        // 3【规则】所有直接持股的股东中有至少一个股东的直接持股比例>=20%且<50%的,取持股比例最大的股东进行判断。根据最大持股比例相同的股东数进行区分判断:
        boolean isTrans3 = false;
        for (InvestmentRelation investmentRelation : resultMap.values()) {
            if (investmentRelation.getInvestmentRatio().compareTo(DigitEnum.RATION020) >= 0 && investmentRelation.getInvestmentRatio().compareTo(DigitEnum.RATION050) < 0) {
                isTrans3 = true;
            }
        }
        if (isTrans3) {
            List<InvestmentRelation> resultList = new ArrayList<>(resultMap.size());
            resultList.addAll(resultMap.values());
            Collections.sort(resultList, (o1, o2) -> o2.getInvestmentRatio().compareTo(o1.getInvestmentRatio()));

            int size = resultList.size();
            // a 如果直接持股比例最大的股东仅有一个,则该直接股东为控空股股东。
            if (size == 1) {
                resultList.get(0).isControllingShareholder = 1;
            } else if (size > 1 && resultList.get(0).getInvestmentRatio().compareTo(resultList.get(1).getInvestmentRatio()) != 0) {
                // 说明 倒序 第一和第二不相等，符合a条件
                resultList.get(0).isControllingShareholder = 1;
            } else if (size > 1 && resultList.get(0).getInvestmentRatio().compareTo(resultList.get(1).getInvestmentRatio()) == 0) {
                // 如果同时存在多个股东持有同等比例,则进行下一轮判断。
                BigDecimal maxRatio = resultList.get(0).getInvestmentRatio();
                int end = Math.min(5, size);
                for (int i = 0; i < size; i++) {

                    if (maxRatio.compareTo(resultList.get(i).getInvestmentRatio()) > 0) {
                        end = Math.min(end, i);
                        break;
                    }

                }

                // 取前5个，为了防止角标越界

                boolean isAtLeastOnePerson = false;
                for (int i = 0; i < end; i++) {
                    // 证明至少有一个是自然人
                    if (resultList.get(i).ifInvestorIsPerson()) {
                        isAtLeastOnePerson = true;
                        break;
                    }
                }

                // 全部都不是自然人,则每个均为控股股东
                if (!isAtLeastOnePerson) {
                    for (int i = 0; i < end; i++) {
                        resultList.get(i).isControllingShareholder = 1;
                    }
                }

                // 部分非自然人、部分自然人
                if (isAtLeastOnePerson) {
                    for (int i = 0; i < end; i++) {
                        // a.非自然人均为控股股东
                        if (resultList.get(i).ifNotInvestorIsPerson()) {
                            resultList.get(i).isControllingShareholder = 1;
                        } else if (resultList.get(i).ifCompanyItemPositions("董事长", "执行董事")) {
                            resultList.get(i).isControllingShareholder = 1;
                        }
                    }
                }
            }

        }

    }

    /**
     * 修复root到resultMap的岗位信息
     *
     * @param resultMap map
     * @param root      root
     */
    public static void fixPositions(Map<String, InvestmentRelation> resultMap, InvestmentRelation root, PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper) {
        resultMap.values().stream().filter(x -> x.getEnd() == 1).filter(InvestmentRelation::ifInvestorIsPerson).forEach(x -> doFixPositions(x, root, personnelEmploymentHistoryMapper));
    }

    /**
     * 修复root到resultMap的岗位信息
     *
     * @param investmentRelation               investmentRelation
     * @param root                             root
     * @param personnelEmploymentHistoryMapper mapper
     */
    public static void doFixPositions(InvestmentRelation investmentRelation, InvestmentRelation root, PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper) {
        Long shareholderNameId = investmentRelation.getShareholderNameId();
        Long edCompanyId = root.getCompanyIdInvested();

        if (isNotZeroOrNull(shareholderNameId) && isNotZeroOrNull(edCompanyId)) {
            String position = personnelEmploymentHistoryMapper.queryPositions(String.valueOf(edCompanyId), String.valueOf(shareholderNameId));
            investmentRelation.setShareholderCompanyPositionListClean(position);
        }

    }

    private static boolean isNotZeroOrNull(Long num) {
        return num != null && num != 0L;
    }

    /**
     * 补充受益所有人
     */
    public static void fixIsUltimate(Map<String, InvestmentRelation> resultMap, InvestmentRelation root, CompanyLegalPersonMapper companyLegalPersonMapper, PersonnelEmploymentHistoryMapper personnelEmploymentHistoryMapper) {
        // 符合001名单和特定企业类型的，不进行收益所有人计算
        if (!ForUltimater.contains(root.getCompanyUsccPrefixCodeTwoInvested())) {
            return;
        }
        if (!isForUltimateByType(root.getCompanyUsccPrefixCodeTwoInvested())) {
            return;
        }

        if (isFaDaiForUltimate(root.getCompanyUsccPrefixCodeTwoInvested(), root.getCompanyTypeInvested()) || isFuZeRenForUltimate(root.getCompanyTypeInvested())) {
            // 取法代作为最终受益人  // 取负责人作为最终受益人
            setPositionForUltimate(resultMap, root);
            return;
        }
//        CompanyLegalPersonMapper companyLegalPersonMapper = JDBCRunner.getMapper(CompanyLegalPersonMapper.class, "9349c027b3b4414aa5f9019cd218e7a3in01/company_base.xml");
        if (root.getIsPartnershipCompanyInvested() == 1) {
            Set<String> links = getCompanyPartnership(root.getCompanyIdInvested(), companyLegalPersonMapper);
            for (String link : links) {
                InvestmentRelation legalRepItermFinalTemp = buildNewInvestmentRelationWithRation0(root, link);
                InvestmentRelation legalRepItermFinal = resultMap.computeIfAbsent(
                        legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), s -> legalRepItermFinalTemp);
                legalRepItermFinal.setEnd(1);
                legalRepItermFinal.setIsUltimate(1);
                resultMap.put(legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), legalRepItermFinal);
            }
            return;
        }
        boolean isAllLess25Flag = true;
        if (resultMap.size() == 0) {
            return;
        }
        ArrayList<InvestmentRelation> values = new ArrayList<>(resultMap.values());
        for (InvestmentRelation value : values) {
            if (value == null || Optional.ofNullable(value.getEnd()).orElse(0) != 1) {
                continue;
            }
            if (PathFormatter.allPathTotalPercent(value).floatValue() >= 0.25) {
                isAllLess25Flag = false;
                if (value.getShareholderEntityType() == 1) {
                    // 法定代表人设置为最终受益人
                    setPositionForUltimate(resultMap, root);
                } else if (value.getShareholderEntityType() == 2) {
                    value.setIsUltimate(1);
                }
            }
        }
        if (isAllLess25Flag) {
            // 所有最终股东都小于25%
            // 法定代表人设置为最终受益人
            setPositionForUltimate(resultMap, root);
            List<Map<String, Object>> allPositionsByCompanyId = personnelEmploymentHistoryMapper.findAllPositionsByCompanyId(root.getCompanyIdInvested());
            for (Map<String, Object> map : allPositionsByCompanyId) {
                if (Pattern.matches(".*(高管|财务|秘书).*", map.get("position_list").toString())) {
                    String gid = map.get("human_name_id").toString();
                    String humanName = map.get("human_name").toString();
                    InvestmentRelation legalRepItermFinalTemp = buildNewInvestmentRelationWithRation0(root, humanName + ":" + gid + "-" + root.getCompanyIdInvested() + "::human");
                    InvestmentRelation legalRepItermFinal = resultMap.computeIfAbsent(legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), s -> legalRepItermFinalTemp);
                    legalRepItermFinal.setIsUltimate(1);
                    legalRepItermFinal.setEnd(1);
                    resultMap.put(legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), legalRepItermFinal);
                }
            }
        }
    }

    public static Set<String> getCompanyPartnership(Long companyId, CompanyLegalPersonMapper companyLegalPersonMapper) {
        Queue<Long> queue = new LinkedList<>();
        queue.add(companyId);
        HashSet<Long> gids = new HashSet<>();
        // 内链
        HashSet<String> result = new HashSet<>();
        while (!queue.isEmpty()) {
            Long currentCompanyId = queue.poll();
            List<CompanyLegalPerson> companyLegalPersons = companyLegalPersonMapper.findCompanyLegalPerson(currentCompanyId);
            for (CompanyLegalPerson companyLegalPerson : companyLegalPersons) {
                if (companyLegalPerson.getLegalRepType() == 1) {
                    if (!gids.contains(companyLegalPerson.getLegalRepNameId())) {
                        // 不成环，继续穿透
                        gids.add(companyLegalPerson.getLegalRepNameId());
                        queue.add(companyLegalPerson.getLegalRepNameId());
                    }
                } else if (companyLegalPerson.getLegalRepType() == 2) {
                    // 做成内链返回
                    result.add(companyLegalPerson.getLegalRepName() + ":" + companyLegalPerson.getLegalRepNameId()
                            + "-" + currentCompanyId + ":" + companyLegalPerson.getLegalRepHumanId() + ":human");
                }
            }
        }
        return result;
    }

    // 设置法定代表人、负责人为中最终受益人
    public static void setPositionForUltimate(Map<String, InvestmentRelation> resultMap, InvestmentRelation root) {
        String legalRepItermLinks = root.getLegalRepInlinksInvested();
        InvestmentRelation legalRepItermFinalTemp = buildNewInvestmentRelationWithRation0(root, legalRepItermLinks);
        InvestmentRelation legalRepItermFinal = resultMap.computeIfAbsent(legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), s -> legalRepItermFinalTemp);
        legalRepItermFinal.setEnd(1);
        legalRepItermFinal.setIsUltimate(1);
        resultMap.put(legalRepItermFinalTemp.getKey(root.getCompanyIdInvested()), legalRepItermFinal);
    }

    // 根据企业类型判单是否计算受益人
    public static boolean isForUltimateByType(String companyTwoCreditCode) {
        return companyTwoCreditCode != null && !Pattern.matches("^(A|12|2|4).*", companyTwoCreditCode);
    }

    // 符合指定企业类型的，按照法定代表人作为收益所有人
    public static boolean isFaDaiForUltimate(String companyTwoCreditCode, String companyType) {
        if (companyTwoCreditCode.equals("92") || companyTwoCreditCode.equals("93")) {
            return true;
        }
        if (companyType != null) {
            if (Pattern.matches(".*?(个体|个人|自自然然人人|私营独资|独资私营|家庭经营).*", companyType) && !companyType.contains("非个人独资")) {
                return true;
            }
        }
        if (Pattern.matches(".*?(律师事务所|会计师事务所).*", companyType)) {
            return true;
        }
        return false;

    }

    // 外国分支结构按负责人作为收益所有人
    public static boolean isFuZeRenForUltimate(String companyType) {
        if (Pattern.matches("^外国.*(分|驻|境内).*", companyType)) {
            if (!NotContainForeignCompanyType.contains(companyType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 修复遍历后的数据中的实际控制人
     *
     * @param resultMap 结果map
     * @param root      root
     */
    public static void fixIsController(Map<String, InvestmentRelation> resultMap, InvestmentRelation root) {

        /**
         * 过滤起点公司的统一社会信用代码
         */
        String edCompanyTwoCreditCode = root.getCompanyUsccPrefixCodeTwoInvested();
        if (!TwoCreditCodeEnum.ForContrller.contains(edCompanyTwoCreditCode)) {
            return;
        }

        // 上市公司的处理逻辑
        if (root.getIsListedCompanyInvested() == 1) {
            String listedCompanyActualController = root.getListedCompanyActualControllerInvested();
            if (StringUtils.isNotBlank(listedCompanyActualController)) {
                String[] split = listedCompanyActualController.split(",");
                for (String inline : split) {
                    InvestmentRelation investmentRelationWithRation0 = buildNewInvestmentRelationWithRation0(root, inline);
                    if (resultMap.containsKey(investmentRelationWithRation0.getKey(root.getCompanyIdInvested()))) {
                        resultMap.get(investmentRelationWithRation0.getKey(root.getCompanyIdInvested())).setIsController(1);
                    } else {
                        investmentRelationWithRation0.setIsController(1);
                        resultMap.put(investmentRelationWithRation0.getKey(root.getCompanyIdInvested()), investmentRelationWithRation0);
                    }
                }
                return;
            }
        }

        // 合伙企业的处理逻辑
        if (root.getIsPartnershipCompanyInvested() == 1) {
            String legalRepItermLinks = root.getLegalRepInlinksInvested();
            if (StringUtils.isNotBlank(legalRepItermLinks)) {
                String[] split = legalRepItermLinks.split(",");
                for (String inline : split) {
                    InvestmentRelation investmentRelationWithRation0 = buildNewInvestmentRelationWithRation0(root, inline);
                    if (resultMap.containsKey(investmentRelationWithRation0.getKey(root.getCompanyIdInvested()))) {
                        resultMap.get(investmentRelationWithRation0.getKey(root.getCompanyIdInvested())).setIsController(1);
                    } else {
                        investmentRelationWithRation0.setIsController(1);
                        resultMap.put(investmentRelationWithRation0.getKey(root.getCompanyIdInvested()), investmentRelationWithRation0);
                    }
                }
            }
            return;
        }

        // 大于等于 50%的count
        long countGe50 = resultMap.entrySet().stream().filter(entry -> entry.getValue().getEnd() == 1).filter(entry -> entry.getValue().ifInvestmentRatioGreaterEqualThan(DigitEnum.RATION050)).count();

        if (countGe50 == 1) { // 预测持股比例>=50%以上的是一个的话
            resultMap.entrySet().stream().filter(entry -> entry.getValue().getEnd() == 1).filter(entry -> entry.getValue().ifInvestmentRatioGreaterEqualThan(DigitEnum.RATION050)).forEach(x -> x.getValue().setIsController(1));
            return;
        }

        long countEq50 = resultMap.entrySet().stream().filter(entry -> entry.getValue().getEnd() == 1).filter(entry -> entry.getValue().getInvestmentRatio().compareTo(DigitEnum.RATION050) == 0).count();

        if (countEq50 == 2) { // 预测持股比例=50%的是正好2个的话
            int allNotPersonCount = 0;
            for (InvestmentRelation rel : resultMap.values()) {
                if (!rel.ifInvestorIsPerson() && rel.getEnd() == 1) {
                    allNotPersonCount++;
                }
            }
            if (allNotPersonCount == 2) { // 全都是非自然人的话
                for (InvestmentRelation value : resultMap.values()) {
                    if (!value.ifInvestorIsPerson() && value.getEnd() == 1) {
                        value.setIsController(1);
                    }
                }
                return;
            } else if (allNotPersonCount == 1) {  // 一个自然人和一个非自然人
                for (InvestmentRelation value : resultMap.values()) {
                    if (value.ifInvestorIsPerson() && value.getEnd() == 1 && value.ifCompanyItemPositions("董事长", "执行董事")) {
                        value.setIsController(1);
                    }
                    if (!value.ifInvestorIsPerson() && value.getEnd() == 1) {
                        value.setIsController(1);
                    }
                }
                return;
            } else { // 两个都自然人
                for (InvestmentRelation value : resultMap.values()) {
                    if (value.ifInvestorIsPerson() && value.getEnd() == 1 && value.ifCompanyItemPositions("董事长", "执行董事")) {
                        value.setIsController(1);
                        return;
                    }
                }
            }
        } // end with 2 sharedHolders


        boolean existsGreaterEqThan030flag = false; // 已经没有超过50%的股东
        for (InvestmentRelation value : resultMap.values()) {
            if (value.getEnd() == 1 && value.getInvestmentRatio().compareTo(DigitEnum.RATION030) >= 0) {
                existsGreaterEqThan030flag = true;
                break;
            }
        }

        if (existsGreaterEqThan030flag) {
            Collection<InvestmentRelation> values = resultMap.values();
            List<InvestmentRelation> newList = new LinkedList<>();
            for (InvestmentRelation value : values) {
                if (value.getEnd() == 1) newList.add(value.clone());
            }
            newList.sort(new Comparator<InvestmentRelation>() {
                @Override
                public int compare(InvestmentRelation o1, InvestmentRelation o2) {
                    return o2.getInvestmentRatio().compareTo(o1.getInvestmentRatio());
                }
            });

            InvestmentRelation investmentRelationWithMaxRatio = newList.get(0);

            int maxRationCount = (int) newList.stream().filter(x -> x.getInvestmentRatio().compareTo(investmentRelationWithMaxRatio.getInvestmentRatio()) == 0).count();

            if (maxRationCount == 1) { // 最大持股股东一人
                investmentRelationWithMaxRatio.setIsController(1);
                resultMap.put(investmentRelationWithMaxRatio.getKey(root.getCompanyIdInvested()), investmentRelationWithMaxRatio);
                return;
            }

            List<InvestmentRelation> topThree = newList.subList(0, maxRationCount); // 获取最多前三个元素
            int notPersoonCnt = 0;
            for (InvestmentRelation rel : topThree) {
                if (!rel.ifInvestorIsPerson()) {
                    notPersoonCnt++;
                }
            }

            if (notPersoonCnt == maxRationCount) { // 全都是非自然人
                for (InvestmentRelation value : topThree) {
                    value.setIsController(1);
                    resultMap.put(value.getKey(root.getCompanyIdInvested()), value);
                }
            } else {// 部分自然人，部分非自然人 && 全都是自然人
                for (InvestmentRelation value : topThree) {
                    if (value.ifInvestorIsPerson() && value.ifCompanyItemPositions("董事长", "执行董事")) {
                        value.setIsController(1);
                        resultMap.put(value.getKey(root.getCompanyIdInvested()), value);
                    }
                    if (!value.ifInvestorIsPerson()) {
                        value.setIsController(1);
                        resultMap.put(value.getKey(root.getCompanyIdInvested()), value);
                    }
                }
            }
        }
    }

    /**
     * 构造 ratio 为 0 的路径信息
     *
     * @param root   root
     * @param inLink inLink
     */
    private static InvestmentRelation buildNewInvestmentRelationWithRation0(InvestmentRelation root, String inLink) {
        InvestmentRelation newInvestmentRelation = new InvestmentRelation();
        newInvestmentRelation.setCompanyIdInvested(root.getCompanyIdInvested());
        newInvestmentRelation.setCompanyTypeInvested(root.getCompanyTypeInvested());
        newInvestmentRelation.setIsListedCompanyInvested(root.getIsListedCompanyInvested());
        newInvestmentRelation.setIsForeignBranchesInvested(root.getIsForeignBranchesInvested());
        newInvestmentRelation.setListedCompanyActualControllerInvested(root.getListedCompanyActualControllerInvested());
        newInvestmentRelation.setIsPartnershipCompany(root.getIsPartnershipCompanyInvested());
        newInvestmentRelation.setLegalRepInlinksInvested(root.getLegalRepInlinksInvested());
        newInvestmentRelation.setCompanyUsccPrefixCodeTwoInvested(root.getCompanyUsccPrefixCodeTwoInvested());
        newInvestmentRelation.setCompanyNameInvested(root.getCompanyNameInvested());

        newInvestmentRelation.setId(null);
        newInvestmentRelation.setShareholderEntityType(getShareholderTypeFromInLink(inLink));
        newInvestmentRelation.setCompanyEntityInlink(inLink);
        newInvestmentRelation.setIsUltimate(0);
        newInvestmentRelation.setIsController(0);
        newInvestmentRelation.setIsControllingShareholder(0);
        newInvestmentRelation.setIsBigShareholder(0);
        newInvestmentRelation.setShareholderNameId(getShareholderGraphIdFromInvestmentRelation(getShareholderTypeFromInLink(inLink), inLink));

        newInvestmentRelation.setInvestmentRatio(DigitEnum.RATION000);
        newInvestmentRelation.setPath(buildNewSinglePath(newInvestmentRelation));

        return newInvestmentRelation;

    }

    /**
     * 构造单一的持股路径
     *
     * @param investmentRelation 关系
     */
    private static Path buildNewSinglePath(InvestmentRelation investmentRelation) {
        InvestmentRelation newInvestmentRelation = investmentRelation.clone();
        Path singlePath = new Path();
        LinkedList<InvestmentRelation> linkedList = new LinkedList<>();
        linkedList.add(newInvestmentRelation);
        LinkedList<LinkedList<InvestmentRelation>> struct = new LinkedList<>();
        struct.add(linkedList);
        singlePath.setStruct(struct);
        return singlePath;
    }

    /**
     * 通过内链获取实体的类型
     *
     * @param inLink 内链
     */
    private static Integer getShareholderTypeFromInLink(String inLink) {
        String[] split = inLink.split(":", -1);
        if (split.length == 4 && split[3].equals("human")) return EntityTypeEnum.PERSON;
        else if (split.length == 3 && split[2].equals("company")) return EntityTypeEnum.COMPANY;
        else return 3;
    }

    /**
     * @param element   Map.Entry<String, InvestmentRelation>
     * @param root      初始节点root
     * @param positions 岗位串，多个按照逗号分割
     */
    private static boolean judgePositions(Map.Entry<String, InvestmentRelation> element, InvestmentRelation root, String positions) {
        return false;
    }


    /**
     * 停止穿透规则-这里停止规则不含层数，包括以下情况
     * 1. 股东是人了 done
     * 2. 自我持股情况  判定同成环
     * 3. 交叉持股 判定同成环
     * 4. 001名单
     * 5. 成环 按照path 和 存储set 的终点来判定
     * 6. 单条路径上比例小于0.05
     * 注意成环的位置会返回不同的值
     *
     * @param investmentRelation    股东信息
     * @param curLevel              当前的层数信息
     * @param resultMap             路径map
     * @param newInvestmentRelation 构造的路径服务于成环的检测
     * @param companyId             root
     * @return 是否需要停止
     */
    public static Integer shouldStop(InvestmentRelation investmentRelation, int curLevel, Map<String, InvestmentRelation> resultMap, InvestmentRelation newInvestmentRelation, Long companyId) {
        // 注吊销公司打断穿透
        String regStatus = new JdbcTemplate("435.company_base")
                .queryForObject("select company_registation_status from company_index where company_id = " + SqlUtils.formatValue(investmentRelation.getShareholderNameId()), rs -> rs.getString(1));
        if (StringUtils.containsAny(regStatus, "注销", "吊销"))
            return ShareHolderStopEnum.STOP_NOT_ADD; // 不加到构造的路径上 不加比例
        // 国旺的逻辑
        if (newInvestmentRelation.ifShareholderPercentLessThan005() || newInvestmentRelation.getShareholderNameId().equals(companyId))
            return ShareHolderStopEnum.STOP_NOT_ADD; // 不加到构造的路径上 不加比例
        if (investmentRelation.ifInvestorIsPerson() || investmentRelation.ifInvestorIn001())
            return ShareHolderStopEnum.STOP_ADD_PATH_RATIO; // 加入到构造的路径中 加比例
        if (cyclicReferenceDetected(resultMap, newInvestmentRelation, companyId))
            return ShareHolderStopEnum.STOP_ADD_PATH_ONLY; // 加入到构造的路径中 不加比例
        return ShareHolderStopEnum.CONTINUE;
    }

    /**
     * 判定是否存在环
     *
     * @param resultMap             路径map
     * @param newInvestmentRelation 新构造的路径
     * @param companyId             root
     * @return 是否存在环
     */
    private static boolean cyclicReferenceDetected(Map<String, InvestmentRelation> resultMap, InvestmentRelation newInvestmentRelation, Long companyId) {
        return newInvestmentRelation.doCyclicReferenceDetected();
    }

    /**
     * 把股东的信息添加到路径 set中
     * 保持大股东标签，控股股东标签不变
     *
     * @param resultMap             路径map
     * @param newInvestmentRelation 新的路径
     * @param edCompanyId           root.companyIdInvested
     * @param shouldStopFlag        flag
     */

    public static void addRatioPathCompanyIntoResMap(Map<String, InvestmentRelation> resultMap, InvestmentRelation newInvestmentRelation, Long edCompanyId, int shouldStopFlag) {
        if (resultMap.containsKey(newInvestmentRelation.getKey(edCompanyId))) {
            InvestmentRelation preInvestmentRelation = resultMap.get(newInvestmentRelation.getKey(edCompanyId));
            if (shouldStopFlag != ShareHolderStopEnum.STOP_ADD_PATH_ONLY) {
                // setPercent
                BigDecimal newPercent = newInvestmentRelation.getInvestmentRatio();
                BigDecimal prePercent = preInvestmentRelation.getInvestmentRatio();
                BigDecimal addDecimal = newPercent.add(prePercent);
                // 产品需求-不允许超过100%
                if (addDecimal.compareTo(DigitEnum.RATION100) > 0) {
                    preInvestmentRelation.setInvestmentRatio(DigitEnum.RATION100);
                } else {
                    preInvestmentRelation.setInvestmentRatio(addDecimal);
                }
            }
            // setPath
            Path prePath = preInvestmentRelation.getPath();
            prePath.mergeNewPath(newInvestmentRelation);
        } else {
            resultMap.put(newInvestmentRelation.getKey(edCompanyId), newInvestmentRelation);
        }
    }

    /**
     * 构造root节点
     *
     * @param companyId companyId
     * @param mapper    mapper
     */
    public static InvestmentRelation buildRootInvestmentRelation(Long companyId, InvestmentRelationMapper mapper) throws BuildRootException {
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
     * 转换investmentRelation 为  ratio_path_company
     *
     * @param investmentRelation inv
     */
    public static RatioPathCompany convertInvestmentRelationToRatioPathCompany(InvestmentRelation investmentRelation) {
        RatioPathCompany ratioPathCompany = new RatioPathCompany();
        ratioPathCompany.setId(investmentRelation.getId());
        ratioPathCompany.setCompanyId(investmentRelation.getCompanyIdInvested());
        ratioPathCompany.setShareholderId(getShareholderId(investmentRelation.getShareholderEntityType(), investmentRelation.getCompanyEntityInlink()));
        ratioPathCompany.setShareholderEntityType(investmentRelation.getShareholderEntityType());
        ratioPathCompany.setShareholderNameId(investmentRelation.getShareholderNameId());
        ratioPathCompany.setInvestmentRatioTotal(investmentRelation.getInvestmentRatio());
        ratioPathCompany.setIsController(investmentRelation.getIsController());
        ratioPathCompany.setIsUltimate(investmentRelation.getIsUltimate());
        ratioPathCompany.setIsBigShareholder(investmentRelation.getIsBigShareholder());
        ratioPathCompany.setIsControllingShareholder(investmentRelation.getIsControllingShareholder());
        ratioPathCompany.setEquityHoldingPath(investmentRelation.getPath());
        ratioPathCompany.setIsDeleted(0);
        return ratioPathCompany;
    }

    /**
     * 获取所有的受影响公司gid
     *
     * @param companyId
     * @param isUpper
     * @param investmentRelationMapper
     * @return
     */
    public static Set<Long> getAllAffectedCompany(Long companyId, boolean isUpper, InvestmentRelationMapper investmentRelationMapper) {
        HashSet<Long> resultSet = new HashSet<>();

        Deque<CompanyNode> queue = new LinkedList<>();
        queue.addFirst(new CompanyNode(companyId, 1));
        while (!queue.isEmpty()) {
            CompanyNode companyNode = queue.pop();
            if (resultSet.contains(companyNode.companyId) || companyNode.percent <= DigitEnum.RATION005.doubleValue()) {
                continue;
            }
            resultSet.add(companyNode.companyId);
            List<InvestmentRelation> investmentRelations;
            if (isUpper) {
                investmentRelations = investmentRelationMapper.getUpperCompanyByGid(companyNode.companyId);
            } else {
                investmentRelations = investmentRelationMapper.getDownCompanyByGid(companyNode.companyId);
            }
            for (InvestmentRelation investmentRelation : investmentRelations) {
                if (isUpper) {
                    queue.push(new CompanyNode(investmentRelation.getShareholderNameId(), investmentRelation.getInvestmentRatio().floatValue() * companyNode.percent));
                } else {
                    queue.push(new CompanyNode(investmentRelation.getCompanyIdInvested(), investmentRelation.getInvestmentRatio().floatValue() * companyNode.percent));
                }
            }
        }
        return resultSet;
    }

    // 计算合作伙伴影响范围
    public static Set<Long> getAllAffectedCompanyPartnership(Long companyId, CompanyLegalPersonMapper companyLegalPersonMapper) {

        // 计算受影响公司时利用法人向上反查，看有没有执行事务合伙人

        /*
        HashSet<Long> sets = new HashSet<>();
        if (companyId == null) {
            return sets;
        }
        Deque<Long> queue = new LinkedList<>();
        queue.addFirst(companyId);
        while (!queue.isEmpty()) {
            Long firstGid = queue.pop();
            List<Map<String, Object>> resultList = companyLegalPersonMapper.findPartnershipByCompanyId(firstGid);
            for (Map<String, Object> map : resultList) {
                Long executiveGid = (Long) map.get("executive_gid");
                if (!sets.contains(executiveGid)) {
                    sets.add(executiveGid);
                    queue.addFirst(executiveGid);
                }
            }
        }
        return sets;

         */
        return null;
    }

}

class CompanyNode {
    Long companyId;
    double percent;

    public CompanyNode(Long companyId, double percent) {
        this.companyId = companyId;
        this.percent = percent;
    }
}