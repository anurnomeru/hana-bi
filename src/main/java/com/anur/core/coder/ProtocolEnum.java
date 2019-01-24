package com.anur.core.coder;

import com.anur.core.elect.vote.model.Votes;

/**
 * Created by Anur IjuoKaruKas on 1/24/2019
 */
public enum ProtocolEnum {

    VOTE(Votes.class);

    ProtocolEnum(Class clazz) {
        this.clazz = clazz;
    }

    public Class<?> clazz;
}
