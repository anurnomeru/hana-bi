package com.anur.core.coder;

import com.anur.core.elect.model.HeartBeat;
import com.anur.core.elect.model.Votes;
import com.anur.core.elect.model.VotesResponse;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 */
public enum ProtocolEnum {

    /**
     * 候选者发往其他节点的拉票请求
     */
    VOTES_REQUEST(Votes.class),

    /**
     * 收到拉票请求后的回包
     */
    VOTES_RESPONSE(VotesResponse.class),

    /**
     * 心跳包
     */
    HEART_BEAT(HeartBeat.class),

    /**
     * 心跳感染，心跳的回包，会告诉发送心跳方，现在的leader是哪个，以及其世代
     */
    HEART_BEAT_INFECTION(HeartBeat.class) ;

    ProtocolEnum(Class clazz) {
        this.clazz = clazz;
    }

    public Class<?> clazz;
}
