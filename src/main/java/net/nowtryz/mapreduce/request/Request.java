package net.nowtryz.mapreduce.request;

import lombok.RequiredArgsConstructor;
import lombok.Value;
import net.nowtryz.mapreduce.packets.Packet;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Value
@RequiredArgsConstructor
public class Request {
    Date sentDate = new Date();
    CompletableFuture<Response> future = new CompletableFuture<>();
    Packet packet;

    public UUID getRequestId() {
        return this.packet.getRequestId();
    }
}
