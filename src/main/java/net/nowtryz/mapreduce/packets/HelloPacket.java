package net.nowtryz.mapreduce.packets;


import lombok.Value;

import java.util.UUID;

@Value
public class HelloPacket implements Packet.RequestPacket {
    UUID requestId = UUID.randomUUID();
    String name;
    Integer cpuNumber;
    Long ramNumber;
}
