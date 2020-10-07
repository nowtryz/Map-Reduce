package net.nowtryz.mapreduce.packets;

import java.io.Serializable;
import java.util.UUID;

public interface Packet extends Serializable {
    UUID getRequestId();
}
