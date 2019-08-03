package com.anur.core.coordinate.apis.fetch

import com.anur.core.coordinate.operator.CoordinateClientOperator
import com.anur.core.elect.ElectMeta
import com.anur.core.listener.EventEnum
import com.anur.core.listener.HanabiListener
import com.anur.core.struct.coordinate.FetchResponse
import com.anur.io.store.prelog.ByteBufPreLogManager
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 *
 * 日志一致性控制器 Follower 端
 */
object FollowerCoordinateManager : CoordinateFetcher() {

    private val logger = LoggerFactory.getLogger(this.javaClass)

    init {

        /**
         * 当集群恢复完毕（收到Leader恢复完毕通知后）
         */
        HanabiListener.register(EventEnum.RECOVERY_COMPLETE) {
            startToFetchFromLeader()
        }

        /**
         * 当与Leader断开连接
         */
        HanabiListener.register(EventEnum.COORDINATE_DISCONNECT_TO_LEADER) {
            cancelFetchTask()
        }

        /**
         * 当许久没收到Leader心跳
         */
        HanabiListener.register(EventEnum.CLUSTER_INVALID
        ) {
            writeLocker {
                cancelFetchTask()

                // 当集群不可用时，与协调 leader 断开连接
                CoordinateClientOperator.shutDownInstance("集群已不可用，与协调 Leader 断开连接")
            }
        }
    }

    override fun howToConsumeFetchResponse(fetchFrom: String, fetchResponse: FetchResponse) {
        logger.trace("收到节点 {} 返回的 FETCH_RESPONSE", fetchFrom)
        ElectMeta.takeIf { it.isLeader }?.run { this.logger.error("出现了不应该出现的情况！") }
        if (fetchResponse.fileOperationSetSize == 0) return
        ByteBufPreLogManager
            .append(fetchResponse.generation, fetchResponse.read())
    }
}