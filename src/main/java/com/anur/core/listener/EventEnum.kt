package com.anur.core.listener

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 */
enum class EventEnum {

    /**
     * 选举成功
     */
    CLUSTER_VALID,

    /**
     * 当节点没有 leader 时
     */
    CLUSTER_INVALID,

    /**
     * 当集群日志恢复完毕
     */
    RECOVERY_COMPLETE,

    /**
     * 当协调器连接上 leader
     */
    COORDINATE_CONNECT_TO_LEADER,

    /**
     * 当协调器与 leader 断开连接
     */
    COORDINATE_DISCONNECT_TO_LEADER,


    /**
     * 当协调器连接上 某节点
     */
    COORDINATE_CONNECT_TO,

    /**
     * 当协调器与 某节点 断开连接
     */
    COORDINATE_DISCONNECT_TO,
}