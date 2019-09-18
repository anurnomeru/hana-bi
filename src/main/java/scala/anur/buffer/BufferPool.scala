package scala.anur.buffer

import com.anur.config.BufferConfiguration

object BufferPool {

    final val TotalMemory = BufferConfiguration.INSTANCE.getMaxBufferPoolSize

}
