package com.anur.exception


/**
 * Created by Anur IjuoKaruKas on 2019/7/8
 */
class LogException : HanabiException {
    constructor(msg: String) : super(msg)
    constructor(thr: Throwable) : super(thr)
}