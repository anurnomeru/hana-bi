package com.anur.core.struct.coordinate;

import java.nio.ByteBuffer;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.core.struct.base.AbstractTimedStruct;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

/**
 * Created by Anur IjuoKaruKas on 4/8/2019
 *
 * 告知客户端已经可以commit了
 *
 * 集群成员收到 COMMIT 消息时,需要回复一个 COMMIT RESPONSE,表明自己的 commit 进度, leader
 * 则会 cover 自身 commit 进度
 */
public class Commiter extends AbstractTimedStruct {

    public static final int CanCommitGenerationOffset = TimestampOffset + TimestampLength;

    public static final int CanCommitGenerationLength = 8;

    public static final int CanCommitOffsetOffset = CanCommitGenerationOffset + CanCommitGenerationLength;

    public static final int CanCommitOffsetLength = 8;

    public static final int BaseMessageOverhead = CanCommitOffsetOffset + CanCommitOffsetLength;

    public Commiter(GenerationAndOffset canCommitGAO) {
        init(BaseMessageOverhead, OperationTypeEnum.COMMIT, buffer -> {
            buffer.putLong(canCommitGAO.getGeneration());
            buffer.putLong(canCommitGAO.getOffset());
        });
    }

    public Commiter(ByteBuffer byteBuffer) {
        this.buffer = byteBuffer;
    }

    public GenerationAndOffset getCanCommitGAO() {
        return new GenerationAndOffset(buffer.getLong(CanCommitGenerationOffset), buffer.getLong(CanCommitOffsetOffset));
    }

    @Override
    public void writeIntoChannel(Channel channel) {
        channel.write(Unpooled.wrappedBuffer(buffer));
    }

    @Override
    public int totalSize() {
        return size();
    }

    @Override
    public String toString() {
        return "Commiter{ GAO => " + getCanCommitGAO() + " }";
    }
}
