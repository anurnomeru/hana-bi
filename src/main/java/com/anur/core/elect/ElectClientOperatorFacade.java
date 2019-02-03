package com.anur.core.elect;

import com.anur.config.InetSocketAddressConfigHelper.HanabiNode;
import com.anur.core.coder.Coder;
import com.anur.core.coder.ProtocolEnum;
import com.anur.core.elect.model.Votes;
import com.anur.core.util.ChannelManager;
import com.anur.core.util.ChannelManager.ChannelType;

/**
 * Created by Anur IjuoKaruKas on 2/2/2019
 *
 * 操作选举客户端连接的门面
 */
public class ElectClientOperatorFacade {

    public static void askForVote(HanabiNode hanabiNode, int generation, Votes votes) {

        // 确保客户端正在运行，如果不在运行，则重启一下
        ElectClientOperator.getInstance(hanabiNode)
                           .start();

        ChannelManager.getInstance(ChannelType.ELECT)
                      .getChannel(hanabiNode.getServerName())
                      .writeAndFlush(Coder.encodeToByteBuf(ProtocolEnum.CANVASSED, generation, votes));
    }

    //    public static void becomeLeader(HanabiNode hanabiNode, int generation) {
    //
    //    }
}
