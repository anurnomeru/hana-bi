package com.anur.core.coordinate.apis.driver

import com.anur.config.InetSocketAddressConfiguration
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.struct.client.ClientRegister
import com.anur.core.struct.coordinate.CommitResponse
import com.anur.core.struct.coordinate.Commiter
import com.anur.core.struct.coordinate.Register
import com.anur.core.struct.coordinate.RegisterResponse
import com.anur.util.ChannelManager
import com.anur.io.hanalog.prelog.ByteBufPreLogManager
import io.netty.channel.Channel
import org.slf4j.LoggerFactory
import sun.security.provider.MD5
import java.net.SocketAddress
import java.nio.ByteBuffer

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 当节点作为服务节点时
 */
object ServiceServerApisHandler {

    private val logger = LoggerFactory.getLogger(ServiceServerApisHandler::class.java)

    /**
     * 表示有客户端连接进来了
     */
    fun handleRegisterRequest(msg: ByteBuffer, channel: Channel) {
        val clientRegister = ClientRegister(msg)
        val remoteAddress: SocketAddress = channel.remoteAddress()
        val id = channel.id()
        logger.info("业务客户端节点: {} id: {} 已注册到本节点", remoteAddress, id)
        ChannelManager.getInstance(ChannelManager.ChannelType.CLIENT)
                .register(id.asLongText(), channel)
        ApisManager.send(id.asLongText(), RegisterResponse(InetSocketAddressConfiguration.getServerName()), RequestProcessor.REQUIRE_NESS)
    }
}