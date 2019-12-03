package com.anur.engine

import com.anur.engine.storage.core.HanabiCommandBuilder
import com.anur.engine.trx.manager.TrxManager

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 */
fun main() {
//    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", ""))
//    println(EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).hanabiEntry)
//    println(EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).hanabiEntry)
//
//    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 2", "Version 10"))
//    println(EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).hanabiEntry)
//    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 3", ""))
//    println(EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).hanabiEntry)
//
//    Thread.sleep(10000L)

    test1()
}

/**
 * 事务1不提交，提交后，数据将一次性都刷入 LSM
 */
fun test4() {
    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("EXT", "EXT 1", trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Null
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Version 10
    Thread.sleep(500000)
}

/**
 * 所有的插入都将阻塞
 */
fun test3() {
    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1", trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    Thread.sleep(2000)
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Null
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Version 10
    Thread.sleep(500000)
}

/**
 * 简单的插入测试
 */
fun test2() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    Thread.sleep(10000)
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Version 10
}

/**
 * 简单的隔离性测试
 */
fun test1() {
    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1", trx1))

    val trx2 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2", trx2))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// 由于隔离性，查不到
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur", trx2))// 由于隔离性，查不到

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Version 1 已经提交，所以查到这个值
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur", trx2))// 由于 trx1已经提交，所以trx2进入了 未提交部分，所以能查到值为 Version 2

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))// 阻塞

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx2))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// Version 3
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur"))// 空
    Thread.sleep(10000)
}