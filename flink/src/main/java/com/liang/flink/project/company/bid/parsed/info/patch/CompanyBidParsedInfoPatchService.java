package com.liang.flink.project.company.bid.parsed.info.patch;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
public class CompanyBidParsedInfoPatchService {
    private final static Set<String> KEYS = new HashSet<>();

    static {
        KEYS.add("gid");
        KEYS.add("name");
        KEYS.add("amount");
    }

    private final CompanyBidParsedInfoPatchDao dao = new CompanyBidParsedInfoPatchDao();

    public String getContent(String mainId) {
        return dao.queryContent(mainId);
    }

    public List<Map<String, Object>> newJson(Object oldJson) {
        List<Map<String, Object>> maps = new ArrayList<>();
        String oldJsonString = String.valueOf(oldJson);
        if (!oldJsonString.startsWith("[")) {
            return maps;
        }
        List<Object> arrOuter = JsonUtils.parseJsonArr(oldJsonString);
        for (Object o : arrOuter) {
            List<Object> arrInner = ((List<Object>) o);
            for (Object obj : arrInner) {
                Map<String, Object> map = (Map<String, Object>) obj;
                LinkedHashMap<String, Object> finalMap = new LinkedHashMap<>(map);
                finalMap.keySet().removeIf(e -> !KEYS.contains(e));
                maps.add(finalMap);
            }
        }
        return maps;
    }

    public List<Map<String, Object>> deduplicateGidAndName(List<Map<String, Object>> maps) {
        List<Map<String, Object>> res = new ArrayList<>();
        HashSet<String> gidSet = new HashSet<>();
        HashSet<String> nameSet = new HashSet<>();
        for (Map<String, Object> map : maps) {
            String gid = String.valueOf(map.get("gid"));
            String name = String.valueOf(map.get("name"));
            if (nameSet.contains(name)) {
                continue;
            }
            if (!"".equals(gid) && gidSet.contains(gid)) {
                continue;
            }
            res.add(map);
            gidSet.add(gid);
            nameSet.add(name);
        }
        return res;
    }

    public List<Map<String, Object>> newAgentJson(Object sourceAgent) {
        if (!TycUtils.isValidName(sourceAgent)) {
            return new ArrayList<>();
        }
        LinkedHashMap<String, Object> res = new LinkedHashMap<>();
        res.put("gid", dao.getCompanyIdByName(sourceAgent));
        res.put("name", sourceAgent);
        return Collections.singletonList(res);
    }

    public void sink(Map<String, Object> columnMap) {
        dao.sink(columnMap);
    }

    public List<Map<String, Object>> post(String content, String uuid) {
        try (HttpResponse response = HttpUtil.createPost("http://10.99.199.173:10040/linking_yuqing_rank")
                .form("text", content)
                .form("bid_uuid", uuid)
                .timeout(1000 * 60)
                .setConnectionTimeout(1000 * 60)
                .setReadTimeout(1000 * 60)
                .execute()) {
            Map<String, Object> json = JsonUtils.parseJsonObj(response.body());
            Map<String, Object> result = (Map<String, Object>) json.getOrDefault("result", new HashMap<String, Object>());
            return (List<Map<String, Object>>) result.getOrDefault("entities", new ArrayList<Map<String, Object>>());
        } catch (Exception ignore) {
            log.warn("uuid: {}", uuid);
            try (HttpResponse response = HttpUtil.createPost("http://10.99.199.173:10040/linking_yuqing_rank")
                    .form("text", content)
                    .form("bid_uuid", uuid)
                    .timeout(1000 * 60 * 3)
                    .setConnectionTimeout(1000 * 60 * 3)
                    .setReadTimeout(1000 * 60 * 3)
                    .execute()) {
                Map<String, Object> json = JsonUtils.parseJsonObj(response.body());
                Map<String, Object> result = (Map<String, Object>) json.getOrDefault("result", new HashMap<String, Object>());
                return (List<Map<String, Object>>) result.getOrDefault("entities", new ArrayList<Map<String, Object>>());
            } catch (Exception e) {
                log.error("uuid: {}", uuid, e);
            }
        }
        return new ArrayList<>();
    }
}
