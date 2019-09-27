package com.anur.engine.storage

import com.anur.util.HanabiExecutors


/**
 * Created by Anur IjuoKaruKas on 2019/9/25
 */
class FutureX<T>(val supp: () -> T) {
    @Volatile private var result: T? = null
    @Volatile private var miao: ((T) -> Unit)? = null

    init {
        HanabiExecutors.execute(Runnable {
            result = supp.invoke()
            miao?.invoke(result!!)
        })
    }
    fun onComplete(miao: (T) -> Unit) = if (result == null) this.miao = miao else miao.invoke(result!!)
}

fun main() {
    val futureX = FutureX {
        Thread.sleep(100)
        2
    }
    futureX.onComplete { println(it) }

    val futureY = FutureX {
        Thread.sleep(100)
        3
    }
    Thread.sleep(200)
    futureY.onComplete { println(it) }

    Thread.sleep(100000)
}