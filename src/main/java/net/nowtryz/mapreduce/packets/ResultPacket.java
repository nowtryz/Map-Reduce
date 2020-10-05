package net.nowtryz.mapreduce.packets;

import java.util.Map;

public class ResultPacket {
    private final Map<String, Integer> result;

    public ResultPacket(Map<String, Integer> result) {
        this.result = result;
    }

    public Map<String, Integer> getResult() {
        return result;
    }
}
