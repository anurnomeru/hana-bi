package com.anur.core.coder;

import com.anur.core.elect.vote.model.Votes;
import com.anur.core.elect.vote.model.VotesResponse;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 */
public enum ProtocolEnum {

    /**
     * 候选者发往其他节点的拉票请求
     */
    CANVASSED(Votes.class),

    /**
     * 收到拉票请求后的回包
     */
    CANVASSED_RESPONSE(VotesResponse.class),
    ;

    ProtocolEnum(Class clazz) {
        this.clazz = clazz;
    }

    public Class<?> clazz;
}
