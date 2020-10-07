package net.nowtryz.mapreduce.request;

import net.nowtryz.mapreduce.packets.Packet;

import java.util.UUID;

public interface ResultHandler {
    void onReceived(Packet response);
}
