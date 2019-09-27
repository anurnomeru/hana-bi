package com.anur.core.lock.free


/**
 * Created by Anur IjuoKaruKas on 2019/9/24
 */
class TrxHolder(val trxId: Long) {

    /**
     * 该事务所有锁定的 key
     */
    val holdKeys: MutableSet<String> = mutableSetOf()

    /**
     * 该事务所有未执行的子事务
     */
    val undoEvent: MutableMap<String, MutableList<() -> Unit>> = mutableMapOf()

    /**
     * 如果不为空，则代表事务已经准备提交了
     */
    var doWhileCommit: (() -> Unit)? = null
}