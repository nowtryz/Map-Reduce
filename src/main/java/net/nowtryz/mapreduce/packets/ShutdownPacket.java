package net.nowtryz.mapreduce.packets;

import lombok.Value;

import java.util.UUID;

@Value
public class ShutdownPacket implements Packet {
    UUID requestId;
}
