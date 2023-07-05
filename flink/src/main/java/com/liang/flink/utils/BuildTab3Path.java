package com.liang.flink.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class BuildTab3Path implements Serializable {

    /**
     * 实现把字符串转为比例
     *
     * @param percentage 百分比
     */
    public static double convertPercentageToDouble(String percentage) {
        String formattedPercentage = percentage.replace("%", "");
        double result = Double.parseDouble(formattedPercentage) / 100.0;
        return result;
    }

    /**
     * 构造人的node-人只出现在首个
     *
     * @param humanStr 需要构造的人的node
     * @param entity   entity
     */
    public static JSONObject getHumanPoint(String humanStr, JSONObject entity) {
        JSONObject pointNode = new JSONObject();
        pointNode.put("id", humanStr);
        JSONObject properties = new JSONObject();
        properties.put("node_type", 2);
        properties.put("company_id", null);
        properties.put("human_name_id", entity.getLongValue("hid"));
        properties.put("name", entity.getString("name"));
        pointNode.put("properties", properties);
        return pointNode;
    }

    /**
     * 构造公司的node
     *
     * @param entity entity
     */
    public static JSONObject getCompanyPoint(JSONObject entity) {
        JSONObject pointNode = new JSONObject();
        pointNode.put("id", String.valueOf(entity.getLongValue("cid")));
        JSONObject properties = new JSONObject();
        properties.put("node_type", 1);
        properties.put("company_id", entity.getLongValue("cid"));
        properties.put("human_name_id", null);
        properties.put("name", entity.getString("name"));
        pointNode.put("properties", properties);
        return pointNode;
    }

    /**
     * 构造新的边
     *
     * @param prePointNode  前节点
     * @param nextPointNode 后节点
     * @param percent       百分比
     */
    public static JSONObject buildNewEdge(JSONObject prePointNode, JSONObject nextPointNode, String percent) {
        JSONObject edge = new JSONObject();
        edge.put("startNode", prePointNode.getString("id"));
        edge.put("endNode", nextPointNode.getString("id"));

        JSONObject properties = new JSONObject();
        properties.put("equity_ratio", convertPercentageToDouble(percent));
        edge.put("properties", properties);
        return edge;
    }

    public static PathNode buildTab3PathSafe(String shareholderId, String prePath) {
        try {
            PathNode pathNode = buildTab3Path(shareholderId, prePath);
            if (pathNode == null) {
                throw new RuntimeException("解析失败");
            }
            return pathNode;
        } catch (Exception e) {
            System.out.println("解析失败");
            PathNode pathNode = new PathNode();
            pathNode.setCount(0);
            pathNode.setPathStr("");
            return pathNode;
        }
    }

    /**
     * 把投资人和路径都穿进去
     *
     * @param prePath 股东人pid#path 或者是 股东公司gid#path
     */

    public static PathNode buildTab3Path(String shareholderId, String prePath) {
        if (shareholderId == null || prePath == null || shareholderId.isEmpty() || prePath.isEmpty()) {
            return new PathNode(0, "[]");
        }

        // 构造结果
        PathNode pathNode = new PathNode();
        JSONArray jsonArray = JSON.parseArray(prePath);
        JSONArray resArr = new JSONArray();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONArray entityArray = jsonArray.getJSONArray(i);
            JSONArray newEntityArray = new JSONArray();

            for (int j = 2; j < entityArray.size(); j = j + 2) {
                JSONObject entity = entityArray.getJSONObject(j);
                if (entity.containsKey("edges") && !entity.getJSONArray("edges").isEmpty()) { // 处理每个边

                    // 前一点
                    JSONObject prePointNode = null;
                    JSONObject pretEntity = entityArray.getJSONObject(j - 1); // 上一节点
                    if (pretEntity.containsKey("type") && pretEntity.getString("type").equals("human")) {
                        prePointNode = getHumanPoint(shareholderId, pretEntity);
                    } else if (pretEntity.containsKey("type") && pretEntity.getString("type").equals("company")) {
                        prePointNode = getCompanyPoint(pretEntity);
                    }


                    // 下一点
                    JSONObject nextPointNode = null;
                    JSONObject nextEntity = entityArray.getJSONObject(j + 1); // 下一节点
                    if (nextEntity.containsKey("type") && nextEntity.getString("type").equals("human")) {
                        nextPointNode = getHumanPoint(shareholderId, nextEntity);
                    } else if (nextEntity.containsKey("type") && nextEntity.getString("type").equals("company")) {
                        nextPointNode = getCompanyPoint(nextEntity);
                    }

                    // 边
                    JSONArray edgeEntity = entity.getJSONArray("edges");
                    if (edgeEntity.size() >= 1) {
                        JSONObject jsonObject = edgeEntity.getJSONObject(0); // 只有一条
                        String percent = jsonObject.getString("percent");
                        if (prePointNode != null && nextPointNode != null) {
                            if (j == 2) newEntityArray.add(prePointNode);
                            newEntityArray.add(buildNewEdge(prePointNode, nextPointNode, percent));
                            newEntityArray.add(nextPointNode);
                        }
                    }
                }
            }
            resArr.add(newEntityArray);
        }
        pathNode.setPathStr(resArr.toJSONString());
        pathNode.setCount(resArr.size());
        return pathNode;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PathNode implements Serializable {
        int count;
        String pathStr;
    }
}
