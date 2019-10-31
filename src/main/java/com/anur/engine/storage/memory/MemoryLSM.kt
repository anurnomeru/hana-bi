package com.anur.engine.storage.memory

import com.alibaba.fastjson.JSON
import com.anur.engine.api.constant.StorageTypeConst
import com.anur.engine.storage.core.HanabiEntry
import com.anur.util.HanabiExecutors
import org.slf4j.LoggerFactory
import kotlin.collections.HashMap

/**
 * Created by Anur IjuoKaruKas on 2019/10/23
 */
object MemoryLSM {

    private val logger = LoggerFactory.getLogger(MemoryLSM::class.java)
    private val dataKeeper = HashMap<String, HanabiEntry>()

    fun get(key: String): HanabiEntry? {
        return dataKeeper[key]
    }

    fun put(key: String, entry: HanabiEntry) {
        dataKeeper[key] = entry
    }
//
//    init {
//        HanabiExecutors.execute(Runnable {
//            while (true) {
//                Thread.sleep(10000)
//                logger.debug(JSON.toJSONString(dataKeeper))
//            }
//        })
//    }
}
