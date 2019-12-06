package com.anur.engine

import com.anur.engine.storage.core.HanabiCommandBuilder
import com.anur.engine.storage.memory.MemoryLSM
import com.anur.engine.trx.manager.TrxManager
import kotlin.random.Random

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 */
fun main() {

    for (i in 0 until 1000) {
//        test1()
//        test2()
//        test3()
//        test4()
//        test5()
    }

    test6()
}

// 100w 1.48g
// 200w 2.97g
// 优化后 200w -> 2.15g
fun test6() {

    val random = Random(1)
    for (i in 0 until 2000000) {
        EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set(i.toString(), getRandomString(random.nextInt(200))))
    }

    Thread.sleep(10000000)

    MemoryLSM.get("1")
}

fun getRandomString(length: Int): String {
    val str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    val random = Random(1)
    val sb = StringBuffer()
    for (i in 0 until length) {
        val number = random.nextInt(62)
        sb.append(str[number])
    }
    return sb.toString()
}

fun test5() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.setIf("Anur", "fff", ""))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", ""))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 2", "Version 10"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 3", ""))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("EXT 3")
}

/**
 * 事务1不提交，提交后，数据将一次性都刷入 LSM
 */
fun test4() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))

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

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 所有的插入都将阻塞
 */
fun test3() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))

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

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// Null

    Thread.sleep(1000)
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 简单的插入测试
 */
fun test2() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))

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

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 简单的隔离性测试
 */
fun test1() {
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1", trx1))

    val trx2 = TrxManager.allocateTrx()
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2", trx2))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 由于隔离性，查不到
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur", trx2)).expect(null)// 由于隔离性，查不到

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx1))

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 1")// Version 1 已经提交，所以查到这个值
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur", trx2)).expect("Version 2")// 由于 trx1已经提交，所以trx2进入了 未提交部分，所以能查到值为 Version 2

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))// 阻塞

    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.commit(trx2))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 3")// Version 3
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.delete("Anur"))
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
    EngineDataFlowControl.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
}