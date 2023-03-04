package com.liang.common.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class SimpleConstructUtils {
    public static final String TO = "to";
    public static final String UNTIL = "until";

    private SimpleConstructUtils() {
    }

    public static List<Integer> newRange(int start, String rangeMode, int end) {
        end = TO.equals(rangeMode) ? end : (end - 1);
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            list.add(i);
        }
        return list;
    }

    //http://c.biancheng.net/c/ascii/
    //(char)33 ～ (char)126, 是可打印的
    public static List<String> newRange(char start, String rangeMode, char end) {
        end = TO.equals(rangeMode) ? end : (char) (end - 1);
        ArrayList<String> list = new ArrayList<>();
        for (char i = start; i <= end; i++) {
            list.add(String.valueOf(i));
        }
        return list;
    }


    @SafeVarargs
    public static <E> List<E> newList(E... elems) {
        return new ArrayList<E>(Arrays.asList(elems));
    }

    @SafeVarargs
    public static <K, V> Map<K, V> newMap(Tuple2<K, V>... tuple2s) {
        HashMap<K, V> map = new HashMap<>();
        for (Tuple2<K, V> tuple2 : tuple2s) {
            map.put(tuple2.f0, tuple2.f1);
        }
        return map;
    }

    public static <K, V> Tuple2<K, V> kv(K k, V v) {
        return Tuple2.of(k, v);
    }
}
