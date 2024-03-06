package com.liang.flink.project.company.bid.parsed.info.patch;

import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import com.liang.common.util.JsonUtils;
import com.liang.common.util.TycUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
@SuppressWarnings("unchecked")
public class CompanyBidParsedInfoPatchService {
    private final static Set<String> KEYS = new HashSet<>();
    private final static Map<String, String> CODE = new HashMap<>();

    static {
        try {
            // keys
            KEYS.add("gid");
            KEYS.add("name");
            KEYS.add("amount");
            // code
            String json = IOUtils.toString(
                    Objects.requireNonNull(JsonUtils.class.getClassLoader().getResourceAsStream("code.json")),
                    StandardCharsets.UTF_8);
            Map<String, Object> map = JsonUtils.parseJsonObj(json);
            map.forEach((k, v) -> CODE.put(k, (String) v));
        } catch (Exception ignore) {
        }
    }

    private final CompanyBidParsedInfoPatchDao dao = new CompanyBidParsedInfoPatchDao();

    public String getContent(String mainId) {
        return dao.queryContent(mainId);
    }

    /**
     * 调整json格式为[{}, {}, {}], 同时剔除map中无用的key
     */
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
            if (TycUtils.isUnsignedId(gid) && gidSet.contains(gid)) {
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

    public List<Map<String, Object>> query(String uuid) {
        return dao.query(uuid);
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
            log.warn("请求AI接口失败, uuid: {}", uuid);
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
                log.error("请求AI接口失败, uuid: {}", uuid, e);
            }
        }
        return new ArrayList<>();
    }

    public Tuple2<String, String> formatCode(String province, String city) {
        boolean provinceMatch = province.matches("\\d{6}");
        boolean cityMatch = city.matches("\\d{6}");
        // 如果省、市都没数据
        if (!provinceMatch && !cityMatch) {
            return Tuple2.of("", "");
        }
        // 如果省有数据, 市没数据
        else if (provinceMatch && !cityMatch) {
            return Tuple2.of(CODE.getOrDefault(province, ""), "");
        }
        // 如果省没数据, 市有数据
        else if (!provinceMatch) {
            return Tuple2.of(CODE.getOrDefault(city.substring(0, 2) + "0000", ""), CODE.getOrDefault(city, ""));
        }
        // 如果都有数据
        else {
            return province.substring(0, 2).equals(city.substring(0, 2)) ?
                    Tuple2.of(CODE.getOrDefault(province, ""), CODE.getOrDefault(city, "")) :
                    Tuple2.of("", "");
        }
    }

    public void delete(String id) {
        dao.delete(id);
    }

    public List<Map<String, Object>> getMention(String mainId) {
        String mention = dao.getMention(mainId);
        ArrayList<Map<String, Object>> maps = new ArrayList<>();
        Arrays.stream(mention.split(";"))
                .filter(TycUtils::isUnsignedId)
                .distinct()
                .forEach(e -> {
                    HashMap<String, Object> map = new HashMap<>();
                    map.put("gid", e);
                    maps.add(map);
                });
        return maps;
    }
}
