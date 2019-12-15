package com.anur.core.coordinate.apis.driver

import com.anur.config.CoordinateConfiguration
import com.anur.core.common.Resetable
import com.anur.core.coordinate.model.RequestProcessor
import com.anur.core.coordinate.sender.CoordinateSender
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.lock.rentrant.ReentrantReadWriteLocker
import com.anur.core.struct.OperationTypeEnum
import com.anur.core.struct.base.AbstractStruct
import com.anur.core.struct.base.AbstractTimedStruct
import com.anur.util.ChannelManager
import com.anur.util.HanabiExecutors
import com.anur.exception.NetworkException
import com.anur.timewheel.TimedTask
import com.anur.timewheel.Timer
import io.netty.channel.Channel
import io.netty.util.internal.StringUtil
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/10
 *
 * 此管理器负责消息的重发、且保证这种消息类型，在收到回复之前，无法继续发同一种类型的消息
 *
 * 1、消息在没有收到回复之前，会定时重发。
 * 2、那么如何保证数据不被重复消费：我们以时间戳作为 key 的一部分，应答方需要在消费消息后，需要记录此时间戳，并不再消费比此时间戳小的消息。
 */
object ApisManager : ReentrantReadWriteLocker(), Resetable {

    val requestHandlerPool = RequestHandlePool

    /**
     * 重启此类，用于在重新选举后，刷新所有任务，不再执着于上个世代的任务
     */
    override fun reset() {
        this.writeLockSupplier(Supplier {
            logger.debug("ApisManager RESET is triggered")

            requestHandlerPool.clear()
            for ((_, value) in inFlight) {
                for ((_, requestProcessor) in value) {
                    requestProcessor?.cancel()
                }
            }
            inFlight = mutableMapOf()
        })
    }

    init {
        HanabiListener.register(EventEnum.CLUSTER_INVALID) { reset() }
    }

    private val logger = LoggerFactory.getLogger(ApisManager::class.java)

    /**
     * 此 map 用于保存各个 RequestType 应返回什么 Response
     */
    private val ResponseAndRequestType = mutableMapOf<OperationTypeEnum, OperationTypeEnum>()

    /**
     * 此 map 用于保存接收到的信息的时间戳，如果收到旧的请求，则不作处理
     */
    private val receiveLog = mutableMapOf<String, MutableMap<OperationTypeEnum, Long?>>()

    /**
     * 此 map 确保对一个服务发送某个消息，在收到回复之前，不可以再次对其发送消息。（有自动重发机制）
     */
    @Volatile
    private var inFlight = mutableMapOf<String, MutableMap<OperationTypeEnum, RequestProcessor?>>()

    init {
        ResponseAndRequestType[OperationTypeEnum.REGISTER_RESPONSE] = OperationTypeEnum.REGISTER
        ResponseAndRequestType[OperationTypeEnum.FETCH_RESPONSE] = OperationTypeEnum.FETCH
        ResponseAndRequestType[OperationTypeEnum.COMMIT_RESPONSE] = OperationTypeEnum.COMMIT
        ResponseAndRequestType[OperationTypeEnum.RECOVERY_RESPONSE] = OperationTypeEnum.RECOVERY
    }

    /**
     * 接收到消息如何处理
     */
    fun receive(msg: ByteBuffer, typeEnum: OperationTypeEnum, channel: Channel) {
        val requestTimestampCurrent = msg.getLong(AbstractTimedStruct.TimestampOffset)
        val serverName = ChannelManager.getInstance(ChannelManager.ChannelType.COORDINATE).getChannelName(channel)

        // serverName 是不会为空的，但是有一种情况例外，便是服务还未注册时 这里做特殊处理
        when {
            /*
             * 业务先验的条件判断(来自客户端的连接)
             */
            typeEnum == OperationTypeEnum.CLIENT_REGISTER -> ServiceServerApisHandler.handleRegisterRequest(msg, channel)

            /*
             * 业务先验的条件判断(来自协调服务之间的连接)
             */
            typeEnum == OperationTypeEnum.REGISTER -> LeaderApisHandler.handleRegisterRequest(msg, channel)
            serverName == null -> logger.error("没有注册却发来了信息，猜想是过期的消息，或者出现了BUG！")
            writeLockSupplierCompel(Supplier {
                var changed = false
                receiveLog.compute(serverName) { _, timestampMap ->
                    (timestampMap ?: mutableMapOf()).also {
                        it.compute(typeEnum) { _, timestampBefore ->
                            changed = (timestampBefore == null || requestTimestampCurrent > timestampBefore)
                            if (changed) requestTimestampCurrent else timestampBefore
                        }
                    }
                }
                changed
            }) -> try {
                doReceive(serverName, msg, typeEnum, channel)
            } catch (e: Exception) {
                logger.warn("在处理来自节点 {} 的 {} 请求时出现异常，可能原因 {}", serverName, typeEnum, e.message)
                writeLocker {
                    receiveLog.compute(serverName) { _, timestampMap ->
                        (timestampMap ?: mutableMapOf()).also { it.remove(typeEnum) }
                    }
                }
                e.printStackTrace()
            }
        }
    }


    private fun doReceive(serverName: String, msg: ByteBuffer, typeEnum: OperationTypeEnum, channel: Channel) {
        when (typeEnum) {
            OperationTypeEnum.FETCH -> LeaderApisHandler.handleFetchRequest(msg, channel)
            OperationTypeEnum.RECOVERY -> LeaderApisHandler.handleRecoveryRequest(msg, channel)
            OperationTypeEnum.COMMIT -> FollowerApisHandler.handleCommitRequest(msg, channel)
            else -> {
                /*
                *  默认请求处理，也就是 response 处理
                */
                val requestType = ResponseAndRequestType[typeEnum]!!
                if (StringUtil.isNullOrEmpty(serverName)) {
                    throw NetworkException("收到了来自已断开连接节点 " + serverName + " 关于 " + requestType.name + " 的无效 response")
                }

                val requestProcessor = getRequestProcessorIfInFlight(serverName, requestType)
                if (requestProcessor != null) {
                    requestProcessor.complete(msg)
                    removeFromInFlightRequest(serverName, requestType)
                    logger.trace("收到来自节点 {} 关于 {} 的 response", serverName, requestType.name)
                }
            }
        }
    }

    /**
     * 此发送器保证【一个类型的消息】只能在收到回复前发送一次，类似于仅有 1 容量的Queue
     */
    fun send(serverName: String, command: AbstractStruct, requestProcessor: RequestProcessor?): Boolean {
        val typeEnum = command.operationTypeEnum

        return if (getRequestProcessorIfInFlight(serverName, typeEnum) != null) {
            logger.trace("尝试创建发送到节点 {} 的 {} 任务失败，上次的指令还未收到 response", serverName, typeEnum.name)
            false
        } else {
            appendToInFlightRequest(serverName, typeEnum, requestProcessor)
            sendImpl(serverName, command, requestProcessor, typeEnum)
            true
        }
    }

    /**
     * 真正发送消息的方法，内置了重发机制
     */
    private fun sendImpl(serverName: String, command: AbstractStruct, requestProcessor: RequestProcessor?, operationTypeEnum: OperationTypeEnum) {
        if (requestProcessor == null || !requestProcessor.isComplete)
            readLockSupplier(Supplier { if (requestProcessor == null || !requestProcessor.isComplete) CoordinateSender.doSend(serverName, command) })

        if (requestProcessor == null) { // 是不需要回复的类型
            removeFromInFlightRequest(serverName, operationTypeEnum)
        } else {
            val task = TimedTask(CoordinateConfiguration.getFetchBackOfMs().toLong()) { sendImpl(serverName, command, requestProcessor, operationTypeEnum) }
            if (reAppendToInFlightRequest(serverName, operationTypeEnum, task)) {

                logger.trace("正在重发向 {} 发送 {} 的任务", serverName, operationTypeEnum)
                Timer.getInstance()// 扔进时间轮不断重试，直到收到此消息的回复
                        .addTask(task)
            }
        }
    }


    /**
     * 将发送任务添加到 InFlightRequest 中，并注册回调函数
     */
    private fun appendToInFlightRequest(serverName: String, typeEnum: OperationTypeEnum, requestProcessor: RequestProcessor?) {
        writeLockSupplier(Supplier {
            logger.trace("InFlight {} {} => 创建发送任务", serverName, typeEnum)
            inFlight.compute(serverName) { _, enums ->
                (enums ?: mutableMapOf()).also { it[typeEnum] = requestProcessor }
            }
        })
    }

    /**
     * 刷新 InFlightRequest 中的 timedTask
     *
     * 如果任务还未完成，则刷新定时任务成功，其余情况均失败
     */
    private fun reAppendToInFlightRequest(serverName: String, typeEnum: OperationTypeEnum, timedTask: TimedTask): Boolean {
        return writeLockSupplierCompel(Supplier {
            logger.trace("InFlight {} {} => 预设重发定时任务", serverName, typeEnum)
            inFlight[serverName]?.let { it[typeEnum] }?.registerTask(timedTask) ?: false
        })
    }

    /**
     * 用于判断是否还在发送
     *
     * - 如果不在发送则返回null
     * - 如果发送完毕，则返回null
     * - 如果未发送完毕，则返回 requestProcessor
     */
    private fun getRequestProcessorIfInFlight(serverName: String, typeEnum: OperationTypeEnum): RequestProcessor? {
        return readLockSupplier(Supplier {
            val requestProcessor = inFlight[serverName]?.let { it[typeEnum] }
            when {
                requestProcessor == null -> null
                requestProcessor.isComplete -> {
                    HanabiExecutors.execute(Runnable { removeFromInFlightRequest(serverName, typeEnum) })
                    null
                }
                else -> requestProcessor
            }
        })
    }

    /**
     * 将发送任务从 InFlightRequest 中移除
     */
    private fun removeFromInFlightRequest(serverName: String, typeEnum: OperationTypeEnum): Boolean {
        return writeLockSupplierCompel(Supplier {
            var exist = false
            inFlight.compute(serverName) { _, enums ->
                logger.trace("InFlight {} {} => 移除发送任务", serverName, typeEnum)
                val removal = (enums ?: mutableMapOf()).remove(typeEnum)
                exist = removal != null
                removal.takeIf { exist }?.cancel()
                return@compute enums
            }
            exist
        })
    }
}
