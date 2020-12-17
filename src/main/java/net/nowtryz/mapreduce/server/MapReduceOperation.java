package net.nowtryz.mapreduce.server;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.nowtryz.mapreduce.utils.FileSizeUtils.toHumanReadableSize;

@Log4j2
@RequiredArgsConstructor
public class MapReduceOperation {
    public final static int DEFAULT_MAX_CHUNK_SIZE = 600*1024; // 600 KiB
    private final static byte SPACE = (byte) ' ';
    private final CoordinatorServer coordinatorServer;
    private final File file;
    private final int maxChunkSize;

    private int bucketCount = 1;

    public Map<String, Integer> compute() throws IOException {
        this.showFileSize();

        List<Map<String, Integer>> mapList = this.map();
        return this.reduce(mapList);
    }

    private List<Map<String, Integer>> map() throws IOException, CompletionException {
        List<CompletableFuture<Map<String, Integer>>> completableFutures = new LinkedList<>();
        int chunkSize = Math.min(
                (int) (this.file.length() / this.coordinatorServer.getPoolSize() / 2),
                this.maxChunkSize
        );
        int chunkCount = (int) (this.file.length() / chunkSize);

        log.info("Will try to create {} chunks with approximated size of {}", chunkCount, toHumanReadableSize(chunkSize));

        // Create buffer outside of loop to save memory
        byte[] buffer = new byte[chunkSize];
        int read;

        try (
                FileInputStream fis = new FileInputStream(this.file);
                InputStream input = new BufferedInputStream(fis)
        ) {
            // Read each chunk from file
            while (true) {
                // TODO wait until some nodes are available to minimize memory impact

                // Clear buffer
                Arrays.fill(buffer, (byte) 0);
                List<Byte> byteList = new ArrayList<>();

                // read desired chunk size
                read = input.read(buffer);

                // Break if end of file
                if (read < buffer.length) {
                    completableFutures.add(this.sendMapRequest(buffer, byteList));
                    break;
                }

                // Loop to get the end of the last word
                byte[] bufferTemp = new byte[1];
                do {
                    read = input.read(bufferTemp);
                    if (read == 0 || bufferTemp[0] == SPACE) break;
                    if (read > 0) byteList.add(bufferTemp[0]);
                } while (true);
                completableFutures.add(this.sendMapRequest(buffer, byteList));

                // Break if end of file
                if (read == 0) break;
            }

            log.info("Sent {} chunks", completableFutures.size());

            return completableFutures.parallelStream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
        }
    }

    private CompletableFuture<Map<String, Integer>> sendMapRequest(byte[] buffer, List<Byte> byteList) {
        if (byteList.size() == 0)  return this.coordinatorServer.startMapping(new String(buffer));

        byte[] bytes = Arrays.copyOf(buffer, buffer.length + byteList.size());
        for (int i = buffer.length; i < byteList.size(); i++) bytes[i + buffer.length] = byteList.get(i);
        return this.coordinatorServer.startMapping(new String(bytes));
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
