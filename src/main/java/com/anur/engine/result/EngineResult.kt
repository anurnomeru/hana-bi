package com.anur.engine.result

import com.anur.engine.storage.entry.ByteBufferHanabiEntry
import com.anur.exception.UnexpectedException

/**
 * Created by Anur IjuoKaruKas on 2019/11/28
 *
 * 请求进入存储引擎后，执行的结果
 */
open class EngineResult {

    /**
     * 是否操作成功，比如插入失败，则为 false
     */
    var result: Boolean = true

    /**
     * result 为 false 才会有 err
     */
    var err: Throwable? = null

// 仅查询有此部分数据

    /**
     * 查询来自引擎的哪个部分
     */
    var queryExecutorDefinition: QueryerDefinition? = null

    /**
     * 查询结果
     */
    private var hanabiEntry: ByteBufferHanabiEntry? = null

    fun setHanabiEntry(hanabiEntry: ByteBufferHanabiEntry) {
        this.hanabiEntry = hanabiEntry
    }

    fun getHanabiEntry(): ByteBufferHanabiEntry? = hanabiEntry?.takeIf { it.getOperateType() == ByteBufferHanabiEntry.Companion.OperateType.ENABLE }

    fun getHanabiEntryOrigin(): ByteBufferHanabiEntry? = hanabiEntry

    fun expect(str: String?) {
        val value = getHanabiEntry()?.getValue()
        if (value?.equals(str) == false) {
            throw UnexpectedException("预期值为 $str 但实际为 [${value}] 数据来自 $queryExecutorDefinition")
        }
    }
}