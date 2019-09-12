package com.anur.config.common

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
enum class ConfigurationEnum constructor(val key: String, val adv: String) {

    ////////////////////// ExtraConfiguration

    DEBUG_MODE("debug", "是否开启DEBUG模式，该模式下有些限制会比较宽松，便于调试"),

    REELECT("reelect", "是否在有leader的情况下重新触发选举，该模式下便于打断点调试，免得老是触发选举，便于调试"),

    ////////////////////// InetSocketAddressConfiguration

    SERVER_NAME("server.name", "server.name 是本机的服务名，集群内应唯一"),

    CLIENT_ADDR("client.addr", "client.addr 的配置格式应由如下组成：client.addr.{服务名}:{选举leader使用端口号}:{集群内机器通讯使用端口号}"),

    ////////////////////// LogConfiguration

    LOG_INDEX_INTERVAL("log.indexInterval", "log.IndexInterval 是操作日志索引生成时的字节间隔，有助于节省空间，不设定的太小都可"),

    LOG_MAX_INDEX_SIZE("log.maxIndexSize", "log.maxIndexSize 日志文件最大只能为这么大，太大了影响索引效率，日志分片受其影响，如果索引文件满了，那么将会创建新的日志分片。"),

    LOG_MAX_MESSAGE_SIZE("log.maxMessageSize", "log.maxMessageSize 是操作日志最大的大小，它也影响我们的操作 key + value 最大的大小"),

    LOG_MAX_SEGMENT_SIZE("log.maxSegmentSize", "log.maxSegmentSize 日志分片文件大小"),

    ////////////////////// ElectConfiguration

    ELECT_ELECTION_TIMEOUT_MS("elect.electionTimeoutMs", "超过 ms 没收取到 leader 的心跳就会发起选举"),

    ELECT_VOTES_BACK_OFF_MS("elect.votesBackOffMs", "选举期间拉票频率，建议为 electionTimeoutMs 的一半"),

    ELECT_HEART_BEAT_MS("elect.heartBeatMs", "心跳包，建议为 electionTimeoutMs 的一半"),

    ////////////////////// CoordinateConfiguration

    COORDINATE_FETCH_BACK_OFF_MS("coordinate.fetchBackOffMs", "间隔 ms 去 leader 拉取最新的日志"),

    ////////////////////// BufferConfiguration

    BUFFER_MAX_SIZE("buffer.max.size", "缓冲区大小")
    ;
}