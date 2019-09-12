package com.anur.config.common

import com.anur.exception.ApplicationConfigException
import javafx.util.Pair
import java.util.ResourceBundle
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 *
 * 统一操作配置文件入口
 */
open class ConfigHelper {

    companion object {
        @Volatile
        private var resourceBundle: ResourceBundle = ResourceBundle.getBundle("application")
        private const val errorFormatter = "读取application.properties配置异常，异常项目：%s，建议：%s"
    }


    private val readLock: Lock
    private val writeLock: Lock
    private val cache = ConcurrentHashMap<ConfigurationEnum, Any>()


    init {
        val readWriteLock = ReentrantReadWriteLock()
        readLock = readWriteLock.readLock()
        writeLock = readWriteLock.writeLock()
    }

    /**
     * 优先获取缓存中的值，如果获取不到再从配置文件获取
     */
    private fun lockSupplier(configEnum: ConfigurationEnum, supplier: () -> Any): Any {
        val t: Any
        try {
            readLock.lock()
            cache.containsKey(configEnum)

            (if (cache.containsKey(configEnum)) {
                t = cache[configEnum]!!
            } else {
                t = supplier.invoke()
                cache[configEnum] = t
            })

        } finally {
            readLock.unlock()
        }
        return t
    }

    /**
     * 刷新配置
     */
    fun refresh() {
        try {
            writeLock.lock()
            cache.clear()
            resourceBundle = ResourceBundle.getBundle("application")
        } finally {
            writeLock.unlock()
        }
    }

    /**
     * 根据key获取某个配置
     */
    internal fun getConfig(configEnum: ConfigurationEnum, transfer: (String) -> Any?): Any {
        return lockSupplier(configEnum) {
            transfer.invoke(resourceBundle.getString(configEnum.key))
                ?: throw ApplicationConfigException(String.format(errorFormatter, configEnum.key, configEnum.adv))
        }
    }

    /**
     * 根据key模糊得获取某些配置，匹配规则为 key%
     */
    internal fun getConfigSimilar(configEnum: ConfigurationEnum, transfer: (Pair<String, String>) -> Any?): Any {
        return lockSupplier(configEnum) {
            val stringEnumeration = resourceBundle.keys
            val keys = mutableListOf<String>()

            while (stringEnumeration.hasMoreElements()) {
                val k = stringEnumeration.nextElement()
                if (k.startsWith(configEnum.key)) {
                    keys.add(k)
                }
            }

            keys.map {
                transfer.invoke(
                    Pair(
                        if (it.length > configEnum.key.length) it.substring(configEnum.key.length + 1) else it,
                        resourceBundle.getString(it)
                    )) ?: throw ApplicationConfigException(String.format(errorFormatter, configEnum.key, configEnum.adv))
            }
        }
    }
}
