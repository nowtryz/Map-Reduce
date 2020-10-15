package net.nowtryz.mapreduce.server;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static net.nowtryz.mapreduce.utils.FileSizeUtils.toHumanReadableSize;

@Log4j2
@RequiredArgsConstructor
public class MapReduceOperation {
    private final CoordinatorServer coordinatorServer;
    private final File file;

    private int bucketCount = 1;

    public Map<String, Integer> compute() throws IOException {
        this.showFileSize();

        List<Map<String, Integer>> mapList = this.map();
        return this.reduce(mapList);
    }

    private List<Map<String, Integer>> map() throws IOException, CompletionException {
        try (Stream<String> stream = Files.lines(file.toPath())) {
            return stream
                    .parallel()
                    .map(this.coordinatorServer::startMapping)
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
        }
    }

    private Map<String, Integer> reduce(List<Map<String, Integer>> mapList) throws CompletionException {
        this.bucketCount = this.coordinatorServer.getPoolSize() * 2;
        Bucket[] buckets = new Bucket[bucketCount];
        IntStream.range(0, bucketCount).parallel().forEach(i -> buckets[i] = new Bucket());

        mapList.parallelStream().forEach(map -> map.entrySet()
                .parallelStream()
                .forEach(entry -> buckets[this.getPartition(entry.getKey())].put(
                        entry.getKey(),
                        entry.getValue()
                ))
        );

        ConcurrentHashMap<String, Integer> result = new ConcurrentHashMap<>();
        Arrays.stream(buckets)
                .parallel()
                .map(Bucket::getOccurences)
                .map(this.coordinatorServer::startReduce)
                .map(CompletableFuture::join)
                .forEach(result::putAll);

        return result;
    }

    private int getPartition(String key) {
        return (key.hashCode() & Integer.MAX_VALUE) % this.bucketCount;
    }

    private void showFileSize() {
        String size = toHumanReadableSize(file.length());
        log.info(String.format("\"Starting computing of %s (%s)", file.getName(), size));
    }

    @Getter
    private class Bucket {
        private final Map<String, List<Integer>> occurences = new ConcurrentHashMap<>();

        public void put(String key, int value) {
            this.occurences.computeIfAbsent(key, s -> new ArrayList<>()).add(value);
        }
    }
}
