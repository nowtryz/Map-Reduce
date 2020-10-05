package net.nowtryz.mapreduce.mapper;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Mapper {
    public static Map<String, Integer> countWords(String line) {
        Map<String, AtomicInteger> concurrentMap = new ConcurrentHashMap<>();

        Arrays.stream(line
                .toLowerCase()
                .split("[ ,’'.!()?-]+"))
                .parallel()
                .map(s -> concurrentMap.computeIfAbsent(s, ignored -> new AtomicInteger(0)))
                .forEach(AtomicInteger::incrementAndGet);

        return concurrentMap
                .entrySet()
                .stream() // cannot use parallel here : collector is not concurrent
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }
}
