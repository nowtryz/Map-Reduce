package net.nowtryz.mapreduce.packets;

import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
public class ReducePacket implements Packet {
    UUID requestId = UUID.randomUUID();
    Map<String, Integer> result;
}
