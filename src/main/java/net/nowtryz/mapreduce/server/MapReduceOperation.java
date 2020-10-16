package net.nowtryz.mapreduce.server;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
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
    private final static byte SPACE = (byte) ' ';
    private final CoordinatorServer coordinatorServer;
    private final File file;

    private int bucketCount = 1;

    public Map<String, Integer> compute() throws IOException {
        this.showFileSize();

        List<Map<String, Integer>> mapList = this.map();
        return this.reduce(mapList);
    }

    private List<Map<String, Integer>> map() throws IOException, CompletionException {
        List<CompletableFuture<Map<String, Integer>>> completableFutures = new LinkedList<>();
        int chunkCount = this.coordinatorServer.getPoolSize()*2;
        long chunkSize = this.file.length()/chunkCount;
        int red;

        log.info("Will try to create {} chunks with approximated size of {}", chunkCount, toHumanReadableSize(chunkSize));

        try (
                FileInputStream fis = new FileInputStream(this.file);
                InputStream input = new BufferedInputStream(fis)
        ) {
            //faire la boucle pour tout le fichier
            while (true) {
                List<Byte> byteList = new ArrayList<>();
                byte[] buffer = new byte[(int)chunkSize];

                red = input.read(buffer);
                for (byte b : buffer) byteList.add(b);

                //casser si red < buffer
                if (red<buffer.length) {
                    completableFutures.add(this.sendMapRequest(byteList));
                    break;
                }

                if (red == chunkSize) {
                    byte[] bufferTemp = new byte[1];
                    do {
                        red = input.read(bufferTemp);
                        if (red > 0) byteList.add(bufferTemp[0]);
                    } while (red > 0 && bufferTemp[0] != SPACE);
                    completableFutures.add(this.sendMapRequest(byteList));
                } else {
                    completableFutures.add(this.sendMapRequest(byteList));
                    break;
                }
            }

            log.info("Sent {} chunks", completableFutures.size());

            return completableFutures.parallelStream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
        }
    }

    private CompletableFuture<Map<String, Integer>> sendMapRequest(List<Byte> byteList) {
        byte[] bytes = new byte[byteList.size()];
        for (int i = 0; i < byteList.size(); i++) bytes[i] = byteList.get(i);
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
