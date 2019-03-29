package com.anur.core.command.coordinate;

import com.anur.core.command.core.AbstractCommand;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 2019/3/29
 *
 * 用于向协调 Leader 拉取消息
 */
public class Fetcher extends AbstractCommand {

    @Override
    public void writeIntoChannel(Channel channel) {

    }

    @Override
    public int totalSize() {
        return 0;
    }
}
