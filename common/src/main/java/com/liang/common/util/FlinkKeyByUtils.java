package com.liang.common.util;

import com.liang.common.service.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.liang.common.util.SimpleConstructUtils.*;

@Data
@Slf4j
public class FlinkKeyByUtils {
    private FlinkKeyByUtils() {
    }

    public static int calculateParallelism(int key, int parallelism) {
        int maxParallelism = 128;
        return MathUtils.murmurHash(key) % maxParallelism * parallelism / maxParallelism;
    }

    public static TreeMap<Integer, List<Integer>> getParallelismMapper(int parallelism) {
        TreeMap<Integer, List<Integer>> result = new TreeMap<>();
        newRange(0, UNTIL, parallelism).forEach(i -> result.put(i, new ArrayList<>()));
        newRange(0, TO, 5_000_000).forEach(i -> result.get(calculateParallelism(i, parallelism)).add(i));
        return result;
    }

    public static <T> Map<T, Integer> getBalanceMapper(List<T> heavyKeys, int parallelism) {
        List<Integer> targets = new ArrayList<>();
        //计算 Flink 原生 key 分配方式
        TreeMap<Integer, List<Integer>> partitionMapper = FlinkKeyByUtils.getParallelismMapper(parallelism);
        log.info(Lists.of(partitionMapper).map(e -> Tuple2.of(e.f0, e.f1.subList(0, heavyKeys.size()))) + "");
        //计算 Heavy Key 分配方式
        for (int i = 0; i * parallelism < heavyKeys.size(); i++) {
            final int fI = i;
            targets.addAll(Lists.of(partitionMapper).map(tuple2 -> tuple2.f1.get(fI)).toList());
        }
        log.info(Lists.of(heavyKeys).zip(targets) + "");
        return Lists.of(heavyKeys).zip(targets).toMap(tuple2 -> tuple2.f0, tuple2 -> tuple2.f1);
    }
}
