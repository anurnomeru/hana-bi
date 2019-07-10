package com.anur.core.coordinate.apis.driver

import com.anur.config.InetSocketAddressConfigHelper
import com.anur.core.coordinate.apis.LeaderCoordinateManager
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.struct.coordinate.CommitResponse
import com.anur.core.struct.coordinate.Commiter
import com.anur.core.struct.coordinate.FetchResponse
import com.anur.core.struct.coordinate.Fetcher
import com.anur.core.struct.coordinate.Register
import com.anur.core.struct.coordinate.RegisterResponse
import com.anur.core.util.ChannelManager
import com.anur.io.store.log.LogManager
import io.netty.channel.Channel
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 所有协调器的业务分发处理中心
 */
object LeaderApisHandler {

    private val logger = LoggerFactory.getLogger(LeaderApisHandler::class.java)

    /**
     * 从 -> 主
     *
     * 协调子节点向主节点请求 Fetch 消息，主节点需返回一定的消息
     */
    fun handleFetchRequest(msg: ByteBuffer, channel: Channel) {
        val fetcher = Fetcher(msg)
        val serverName = ChannelManager.getInstance(ChannelManager.ChannelType.COORDINATE)
            .getChannelName(channel)

        logger.debug("收到来自协调节点 {} 的 Fetch 请求 {} ", serverName, fetcher.fetchGAO)

        val canCommit = LeaderCoordinateManager.fetchReport(serverName, fetcher.fetchGAO)

        ApisManager.send(serverName, Commiter(canCommit), RequestProcessor(
            { byteBuffer ->
                val commitResponse = CommitResponse(byteBuffer)
                LeaderCoordinateManager.commitReport(serverName, commitResponse.commitGAO)
            }, null))

        // 为什么要。next，因为 fetch 过来的是客户端最新的 GAO 进度，而获取的要从 GAO + 1开始
        val fetchDataInfo = LogManager.getINSTANCE()
            .getAfter(fetcher.fetchGAO
                .next())

        ApisManager.send(serverName, FetchResponse(fetchDataInfo), RequestProcessor.REQUIRE_NESS)
    }

    /**
     * 协调子节点向父节点注册自己
     */
    fun handleRegisterRequest(msg: ByteBuffer, channel: Channel) {
        val register = Register(msg)
        logger.info("协调节点 {} 已注册到本节点", register.getServerName())
        ChannelManager.getInstance(ChannelManager.ChannelType.COORDINATE)
            .register(register.getServerName(), channel)
        ApisManager.send(register.getServerName(), RegisterResponse(InetSocketAddressConfigHelper.getServerName()), RequestProcessor.REQUIRE_NESS)
    }
}