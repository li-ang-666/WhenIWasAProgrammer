package org.tyc.entity;

import com.alibaba.fastjson.JSON;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedList;


/**
 * 股东和被投资公司之间的所有的路径存储
 * 每条路径按照 List<InvestmentRelation> 顺序存储
 * 多个路径按照 struct 中的多个元素来表示
 */

@Data
public class Path implements Cloneable, Serializable {
    public LinkedList<LinkedList<InvestmentRelation>> struct;

    /**
     * Adds a new path link.
     *
     * @param newInvestmentRelation the investment relation to add
     */
    public void addNewPathLink(InvestmentRelation newInvestmentRelation) {
        LinkedList<InvestmentRelation> linkedList = new LinkedList<>();
        linkedList.add(newInvestmentRelation);
        if (struct == null) {
            struct = new LinkedList<LinkedList<InvestmentRelation>>();
        }
        struct.add(linkedList);
    }

    /**
     * Appends a new InvestmentRelation to each path in the structure.
     *
     * @param newInvestmentRelation The InvestmentRelation to be appended.
     */
    public void appendAtEachPath(InvestmentRelation newInvestmentRelation) {
        for (LinkedList<InvestmentRelation> linkedList : struct) {
            linkedList.addLast(newInvestmentRelation);
        }
    }

    /**
     * This method merges a new InvestmentRelation into an existing InvestmentRelation.
     * The new path is added to the existing path by adding all elements of the new path's structure to the existing path.
     *
     * @param newInvestmentRelation The InvestmentRelation to be merged.
     */
    public void mergeNewPath(InvestmentRelation newInvestmentRelation) {
        struct.addAll(newInvestmentRelation.getPath().getStruct());
    }

    /**
     * This method gets the usage count for an investment relation.
     * It goes through each linked list in the struct container and
     * checks if the company entity inlink exists. It increments
     * the count if the company entity exists and returns the
     * count.
     *
     * @param newInvestmentRelation An investment relation to find the usage count of
     * @return The usage count of the investment relation
     */
    public int getUsageCountForInvestmentRelation(InvestmentRelation newInvestmentRelation) {
        int count = 0;
        for (LinkedList<InvestmentRelation> linkedList : struct) {
            count = 0;
            for (InvestmentRelation investmentRelation : linkedList) {
                if (investmentRelation.getCompanyEntityInlink().equals(newInvestmentRelation.getCompanyEntityInlink())) {
                    count++;
                    if (count >= 2) return count;
                }
            }
        }
        return count;
    }

    @Override
    public Path clone() {
        Path newPath = new Path();
        newPath.setStruct(new LinkedList<>());
        // 复制struct
        for (LinkedList<InvestmentRelation> linkedList : this.struct) {
            LinkedList<InvestmentRelation> copyLinkedList = new LinkedList<>();
            for (InvestmentRelation inv : linkedList) {
                copyLinkedList.add(inv.clone());
            }
            newPath.struct.add(copyLinkedList);
        }
        return newPath;
    }

    /**
     * 提供需要的json string的转化
     */
    public String toJsonString() {
        return JSON.toJSONString(this.struct);
    }
}
