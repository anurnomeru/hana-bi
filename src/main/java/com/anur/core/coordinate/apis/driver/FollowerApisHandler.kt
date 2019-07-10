package com.anur.core.coordinate.apis.driver

import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.struct.coordinate.CommitResponse
import com.anur.core.struct.coordinate.Commiter
import com.anur.core.util.ChannelManager
import com.anur.io.store.prelog.ByteBufPreLogManager
import io.netty.channel.Channel
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 从节点协调器的业务分发处理中心
 */
object FollowerApisHandler {

    private val logger = LoggerFactory.getLogger(FollowerApisHandler::class.java)

    /**
     * 主 -> 从
     *
     * 子节点处理来自 leader 的 commit 请求，并 commit 自己的 preLog
     */
    fun handleCommitRequest(msg: ByteBuffer, channel: Channel) {
        val commiter = Commiter(msg)
        val serverName = ChannelManager.getInstance(ChannelManager.ChannelType.COORDINATE)
            .getChannelName(channel)

        logger.debug("收到来自协调 Leader {} 的 commit 请求 {} ", serverName, commiter.canCommitGAO)

        ByteBufPreLogManager.getINSTANCE()
            .commit(commiter.canCommitGAO)

        val commitGAO = ByteBufPreLogManager.getINSTANCE()
            .commitGAO

        ApisManager.send(serverName, CommitResponse(commitGAO), RequestProcessor.REQUIRE_NESS)
    }
}