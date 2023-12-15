package com.liang.flink.project.company.bid.parsed.info.patch;

import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;

import java.util.*;

public class CompanyBidParsedInfoPatchService {
    private final CompanyBidParsedInfoPatchDao dao = new CompanyBidParsedInfoPatchDao();

    @SuppressWarnings("unchecked")
    public String newJson(Object oldJson) {
        String oldJsonString = String.valueOf(oldJson);
        if (!oldJsonString.startsWith("[")) {
            return "[]";
        }
        List<Map<String, Object>> maps = new ArrayList<>();
        List<Object> arrOuter = JsonUtils.parseJsonArr(oldJsonString);
        for (Object o : arrOuter) {
            List<Object> arrInner = ((List<Object>) o);
            for (Object obj : arrInner) {
                Map<String, Object> map = (Map<String, Object>) obj;
                maps.add(map);
            }
        }
        return JsonUtils.toString(maps);
    }

    public String newAgentJson(Object sourceAgent) {
        if (!TycUtils.isValidName(sourceAgent)) {
            return "[]";
        }
        LinkedHashMap<String, Object> res = new LinkedHashMap<>();
        res.put("gid", dao.getCompanyIdByName(sourceAgent));
        res.put("name", sourceAgent);
        return JsonUtils.toString(Collections.singletonList(res));
    }

    public void sink(Map<String, Object> columnMap) {
        dao.sink(columnMap);
    }
}
