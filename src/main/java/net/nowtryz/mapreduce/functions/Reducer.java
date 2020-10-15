package net.nowtryz.mapreduce.functions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public  class Reducer {
    public static Map<String, Integer> reduceOld(List<Map<String, Integer>> mapList) {
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

        mapList.parallelStream().forEach(map -> map.forEach((key, value) -> counts.compute(key, (ignored, atomicInteger) -> {
            if (atomicInteger == null) return new AtomicInteger(value);
            atomicInteger.addAndGet(value);
            return atomicInteger;
        })));

        return counts.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        stringAtomicIntegerEntry -> stringAtomicIntegerEntry.getValue().intValue()
                ));
    }

    public static Map<String, Integer> reduce(Map<String, List<Integer>> mapList) {
        return mapList
                .entrySet()
                .parallelStream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> count(entry.getValue())));
    }

    private static int count(List<Integer> occurrences) {
        AtomicInteger i = new AtomicInteger(0);
        occurrences.parallelStream().forEach(i::addAndGet);
        return i.intValue();
    }
}
