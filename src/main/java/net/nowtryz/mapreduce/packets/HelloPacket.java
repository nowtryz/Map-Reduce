package net.nowtryz.mapreduce.packets;

import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.util.UUID;

@Value
@RequiredArgsConstructor
public class HelloPacket implements Packet.RequestPacket {
    UUID requestId = UUID.randomUUID();
    String name;
    Integer cpuNumber;
    Integer ramNumber;
}
