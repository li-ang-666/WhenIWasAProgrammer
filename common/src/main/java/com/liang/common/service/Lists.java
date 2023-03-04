package com.liang.common.service;

import com.liang.common.util.lambda.*;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.liang.common.util.lambda.LambdaExceptionEraser.*;

public class Lists<E> {
    private List<E> value;

    private Lists() {
    }

    /*---------------------------------------------construct------------------------------------------------------*/

    public static <E> Lists<E> of(Collection<E> collection) {
        Lists<E> lists = new Lists<>();
        lists.value = new ArrayList<>(collection);
        return lists;
    }

    public static <K, V> Lists<Tuple2<K, V>> of(Map<K, V> map) {
        List<Tuple2<K, V>> list = new ArrayList<>();
        map.forEach((k, v) -> list.add(Tuple2.of(k, v)));
        return of(list);
    }

    @SafeVarargs
    public static <E> Lists<E> of(E... elems) {
        return of(Arrays.asList(elems));
    }

    /*---------------------------------------------transform------------------------------------------------------*/

    public <T> Lists<T> map(ThrowingFunction<E, T> function) {
        return of(
                value.parallelStream().map(functionEraser(function)).collect(Collectors.toList())
        );
    }

    public Lists<E> filter(ThrowingPredicate<E> predicate) {
        return of(
                value.parallelStream().filter(predicateEraser(predicate)).collect(Collectors.toList())
        );
    }

    public Lists<E> sort(Comparator<E> comparator) {
        return of(
                value.parallelStream().sorted(comparator).collect(Collectors.toList())
        );
    }

    public <T> Lists<T> flatMap(ThrowingFunction<E, Stream<T>> function) {
        return of(
                value.parallelStream().flatMap(functionEraser(function)).collect(Collectors.toList())
        );
    }

    public <T> Lists<Tuple2<T, List<E>>> groupBy(ThrowingFunction<E, T> function) {
        return of(
                value.parallelStream().collect(Collectors.groupingBy(functionEraser(function)))
        );
    }


    public <T> T aggregate(T init, ThrowingBiFunction<T, E, T> biFunction, ThrowingBinaryOperator<T> binaryOperator) {
        return value.parallelStream().reduce(
                init, biFunctionEraser(biFunction), biMergeEraser(binaryOperator)
        );
    }

    public Lists<E> distinct() {
        return Lists.of(
                new LinkedHashSet<>(value)
        );
    }

    public void forEach(ThrowingConsumer<E> consumer) {
        value.forEach(consumerEraser(consumer));
    }

    public Lists<Tuple2<String, E>> diffDistinct(List<E> list) {
        ArrayList<Tuple2<String, E>> result = new ArrayList<>();
        Set<E> left = new LinkedHashSet<>(value);
        left.removeAll(new LinkedHashSet<>(list));
        left.forEach(e -> result.add(Tuple2.of("L+R-", e)));

        Set<E> right = new LinkedHashSet<>(list);
        right.removeAll(new LinkedHashSet<>(value));
        right.forEach(e -> result.add(Tuple2.of("L-R+", e)));

        return of(result);
    }

    public <V> Lists<Tuple2<E, V>> zip(List<V> list) {
        Map<E, V> map = new LinkedHashMap<>();
        for (int i = 0; i < value.size(); i++) {
            map.put(value.get(i), i < list.size() ? list.get(i) : null);
        }
        return of(map);
    }

    public Lists<Tuple2<E, Integer>> zipWithIndex() {
        Map<E, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < value.size(); i++) {
            map.put(value.get(i), i);
        }
        return of(map);
    }

    public Lists<List<E>> split(int numPerSeg) {
        LinkedList<E> linkedList = new LinkedList<>(value);
        List<List<E>> result = new ArrayList<>();
        while (!linkedList.isEmpty()) {
            ArrayList<E> list = new ArrayList<>();
            for (int i = 0; i < numPerSeg; i++) {
                if (!linkedList.isEmpty()) {
                    list.add(linkedList.removeFirst());
                } else {
                    result.add(list);
                    return of(result);
                }
            }
            result.add(list);
        }
        return of(result);
    }

    public E head() {
        return new LinkedList<>(value).getFirst();
    }

    public Lists<E> tail() {
        LinkedList<E> linkedList = new LinkedList<>(value);
        linkedList.removeFirst();
        return Lists.of(linkedList);
    }

    public E last() {
        return new LinkedList<>(value).getLast();
    }

    public Lists<E> init() {
        LinkedList<E> linkedList = new LinkedList<>(value);
        linkedList.removeLast();
        return Lists.of(linkedList);
    }

    public Lists<E> reverse() {
        ArrayList<E> arrayList = new ArrayList<>(value);
        Collections.reverse(arrayList);
        return Lists.of(arrayList);
    }

    public String mkString(String prefix, String delimiter, String suffix) {
        return value.parallelStream().map(String::valueOf).collect(Collectors.joining(delimiter, prefix, suffix));
    }

    public <K, V> Map<K, V> toMap(ThrowingFunction<E, K> keyMapper, ThrowingFunction<E, V> valueMapper) {
        //Collectors.toMap 坑太多了, 不使用
        LinkedHashMap<K, V> result = new LinkedHashMap<>();
        value.forEach(e -> result.put(functionEraser(keyMapper).apply(e), functionEraser(valueMapper).apply(e)));
        return result;
    }

    public List<E> toList() {
        return new ArrayList<>(value);
    }

    public Set<E> toSet() {
        return new LinkedHashSet<>(new ArrayList<>(value));
    }

    public int size() {
        return value.size();
    }

    public int length() {
        return size();
    }

    @Override
    public String toString() {
        return "Lists" + mkString("[", ", ", "]");
    }

    public boolean isEmpty() {
        return value.isEmpty();
    }

    public boolean nonEmpty() {
        return !value.isEmpty();
    }
}
