package net.nowtryz.mapreduce.request;

import lombok.Value;
import net.nowtryz.mapreduce.packets.Packet;

import java.util.Date;
import java.util.UUID;

@Value
public class Response {
    UUID requestId;
    Date receivedDate = new Date();
    Packet.ResultPacket receivedPacket;
}
