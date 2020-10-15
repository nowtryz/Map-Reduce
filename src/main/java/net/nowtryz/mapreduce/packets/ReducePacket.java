package net.nowtryz.mapreduce.packets;

import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Value
public class ReducePacket implements Packet.RequestPacket {
    UUID requestId = UUID.randomUUID();
    Map<String, List<Integer>> data;
}
