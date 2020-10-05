package net.nowtryz.mapreduce.mapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ThreadMapper implements Mapper {

    @Override
    public CompletableFuture<Map<String, Integer>> accept(String line) {
        return null;
    }
}
