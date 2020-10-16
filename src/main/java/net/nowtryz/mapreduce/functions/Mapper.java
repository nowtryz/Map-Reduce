package net.nowtryz.mapreduce.functions;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Mapper {
    public static Map<String, Integer> countWords(String[] words) {
        Map<String, AtomicInteger> concurrentMap = new ConcurrentHashMap<>();

        // Get stream from array pf words
        Arrays.stream(words)
                // Use parallel stream to use as many cores as we can
                .parallel()
                // get the atomic integer associated with the word in the map
                // if absent, we create an AtomicInteger initialized top 0
                .map(s -> concurrentMap.computeIfAbsent(s, ignored -> new AtomicInteger(0)))
                // We increment the atomic integer
                .forEach(AtomicInteger::incrementAndGet);

        return concurrentMap
                .entrySet()
                .stream() // cannot use parallel here : collector is not concurrent
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    public static String[] explodeLine(String line) {
        return line
                .toLowerCase()
                .split("[\\s,:;’'.!()?-]+");
    }
}
