package scala.anur.buffer

import com.anur.config.BufferConfiguration

/**
 * 后续要加上的 项目内存控制 = = 现在不急（懒得做）
 */
object BufferPool {

    final val TotalMemory = BufferConfiguration.INSTANCE.getMaxBufferPoolSize

}
