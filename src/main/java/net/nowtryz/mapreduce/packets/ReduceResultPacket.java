package net.nowtryz.mapreduce.packets;

import lombok.Value;

import java.util.Map;
import java.util.UUID;

@Value
public class ReduceResultPacket implements Packet.ResultPacket {
    UUID requestId;
    Map<String, Integer> result;
}
