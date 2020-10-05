package net.nowtryz.mapreduce.mapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Mapper {
    CompletableFuture<Map<String, Integer>> accept(String line);
}
