package com.liang.flink.project.company.bid.parsed.info.patch;

import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;

import java.util.*;

public class CompanyBidParsedInfoPatchService {
    private final static Set<String> KEYS = new HashSet<>();

    static {
        KEYS.add("gid");
        KEYS.add("name");
        KEYS.add("amount");
    }

    private final CompanyBidParsedInfoPatchDao dao = new CompanyBidParsedInfoPatchDao();

    @SuppressWarnings("unchecked")
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
}
