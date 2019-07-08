package com.anur.exception


/**
 * Created by Anur IjuoKaruKas on 2019/7/5
 */
open class HanabiException : RuntimeException {
    constructor(msg: String) : super(msg)
    constructor(msg: String, thr: Throwable) : super(msg, thr)
    constructor(thr: Throwable) : super(thr)
}