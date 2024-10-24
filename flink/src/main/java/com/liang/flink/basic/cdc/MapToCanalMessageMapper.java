package com.liang.flink.basic.cdc;

import cn.hutool.core.lang.func.Consumer3;
import cn.hutool.core.util.ObjUtil;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.liang.common.util.JsonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@SuppressWarnings("unchecked")
public class MapToCanalMessageMapper extends RichFlatMapFunction<Map<String, Object>, FlatMessage> {
    private final Map<String, Consumer3<Map<String, String>, Map<String, String>, FlatMessage>> dictionary = new HashMap<>();

    {
        dictionary.put("c", (before, after, flatMessage) -> {
            flatMessage.setType(CanalEntry.EventType.INSERT.name());
            flatMessage.setData(Collections.singletonList(after));
        });
        dictionary.put("u", (before, after, flatMessage) -> {
            flatMessage.setType(CanalEntry.EventType.UPDATE.name());
            flatMessage.setData(Collections.singletonList(after));
            Map<String, String> old = before.entrySet().stream()
                    .filter(entry -> ObjUtil.notEqual(entry.getValue(), after.get(entry.getKey())))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            flatMessage.setOld(Collections.singletonList(old));
        });
        dictionary.put("d", (before, after, flatMessage) -> {
            flatMessage.setType(CanalEntry.EventType.DELETE.name());
            flatMessage.setData(Collections.singletonList(before));
        });
    }

    @Override
    public void flatMap(Map<String, Object> debeziumMap, Collector<FlatMessage> out) {
        try {
            FlatMessage flatMessage = debeziumMapToFlatMessage(debeziumMap);
            out.collect(flatMessage);
        } catch (Exception e) {
            log.error("cdc to canal error, debezium map: {}", JsonUtils.toString(debeziumMap));
        }
    }

    private FlatMessage debeziumMapToFlatMessage(Map<String, Object> debeziumMap) {
        Map<String, String> before = (Map<String, String>) debeziumMap.get("before");
        Map<String, String> after = (Map<String, String>) debeziumMap.get("after");
        Map<String, Object> source = (Map<String, Object>) debeziumMap.get("source");
        FlatMessage flatMessage = new FlatMessage();
        flatMessage.setDatabase((String) source.get("db"));
        flatMessage.setTable((String) source.get("table"));
        flatMessage.setPkNames(Collections.singletonList("id"));
        flatMessage.setIsDdl(false);
        dictionary.get((String) debeziumMap.get("op")).accept(before, after, flatMessage);
        flatMessage.setEs(Long.parseLong((String) source.get("ts_ms")));
        flatMessage.setTs(Long.parseLong((String) debeziumMap.get("ts_ms")));
        return flatMessage;
    }
}
