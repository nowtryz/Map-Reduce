package net.nowtryz.mapreduce.packets;

import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
public class MapResultPacket implements Packet {
    UUID requestId;
    Map<String, Integer> result;
}
