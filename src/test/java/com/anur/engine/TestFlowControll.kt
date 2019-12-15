package com.anur.engine

import com.anur.engine.common.core.HanabiCommandBuilder
import com.anur.engine.common.entry.ByteBufferHanabiEntry
import com.anur.engine.memory.MemoryLSM
import com.anur.engine.trx.manager.TrxManager
import kotlin.random.Random

/**
 * Created by Anur IjuoKaruKas on 2019/10/31
 */
fun main() {
    println(System.currentTimeMillis())
    for (i in 0 until 10000) {
        test1()
        test2()
        test3()
        test4()
        test5()
    }
    println(System.currentTimeMillis())

    test6()
}


val map = HashMap<String, ByteBufferHanabiEntry>()

// 100w 1.48g
// 200w 2.97g
// 优化后 200w -> 2.15g
// 优化后
// 200W -> 1g
// 400W -> 3.22G
// 400W -> 2.13G
fun test6() {

    val random = Random(1)

    Thread.sleep(10000)

    for (i in 0 until 1000000) {
        val key = getRandomString(random.nextInt(5)) + i.toString()
        EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set(key, getRandomString(random.nextInt(200))))
//        map.put(i.toString(), ByteBufferHanabiEntry(ByteBuffer.allocate(random.nextInt(200) + 6)))
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
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.setIf("Anur", "fff", ""))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", ""))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 2", "Version 10"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("")// ""
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.setIf("Anur", "EXT 3", ""))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("EXT 3")
}

/**
 * 事务1不提交，提交后，数据将一次性都刷入 LSM
 */
fun test4() {
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("EXT", "EXT 1", trx1))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 所有的插入都将阻塞
 */
fun test3() {
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1", trx1))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// Null

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.commit(trx1))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 简单的插入测试
 */
fun test2() {
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 4"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 5"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 6"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 7"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 8"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 9"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 10"))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 10")// Version 10
}

/**
 * 简单的隔离性测试
 */
fun test1() {
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))

    val trx1 = TrxManager.allocateTrx()
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 1", trx1))

    val trx2 = TrxManager.allocateTrx()
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 2", trx2))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 由于隔离性，查不到
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur", trx2)).expect(null)// 由于隔离性，查不到

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.commit(trx1))

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 1")// Version 1 已经提交，所以查到这个值
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur", trx2)).expect("Version 2")// 由于 trx1已经提交，所以trx2进入了 未提交部分，所以能查到值为 Version 2

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.set("Anur", "Version 3"))// 阻塞

    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.commit(trx2))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect("Version 3")// Version 3
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.delete("Anur"))
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
    EngineDataFlowController.commandInvoke(HanabiCommandBuilder.select("Anur")).expect(null)// 空
}