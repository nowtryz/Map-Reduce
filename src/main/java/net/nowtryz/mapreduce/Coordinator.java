package net.nowtryz.mapreduce;

import net.nowtryz.mapreduce.functions.Mapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class Coordinator {
    public static void main(String[] args) {
        String fileName = args[0];
        Path path = Paths.get(fileName);


        try (Stream<String> stream = Files.lines(path)) {
            Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();

            stream
                    .parallel()
                    .map(Coordinator::mapper)
                    .map(CompletableFuture::join)
                    .forEach(map -> map.forEach((key, value) -> counts.compute(key, (s, atomicInteger) -> {
                        if (atomicInteger == null) return new AtomicInteger(value);
                        atomicInteger.addAndGet(value);
                        return atomicInteger;
                    })));

            System.out.println(counts);

        } catch (IOException exception) {
            exception.printStackTrace();
        }
    }

    private static CompletableFuture<Map<String, Integer>> mapper(String line) {
        return CompletableFuture.completedFuture(Mapper.countWords(Mapper.explodeLine(line)));
    }
}
