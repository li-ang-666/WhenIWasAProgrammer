package com.liang.flink.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class StreamSinkInfo implements Serializable {
    private String orgId;
    private Long lastHeartBeatTime;

    public StreamSinkInfo(String sinkInfo) {
        String[] split = sinkInfo.split("###");
        this.orgId = split[0];
        this.lastHeartBeatTime = Long.parseLong(split[1]);
    }
}
