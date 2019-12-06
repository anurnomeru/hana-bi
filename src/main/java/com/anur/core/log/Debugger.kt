package com.anur.core.log

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 主要是调试的时候经常要改 logger 的隔离级别太鸡儿麻烦了！
 */
class Debugger(clazz: Class<Any>) {
    private val h: Logger = LoggerFactory.getLogger(clazz)
    private var level: DebuggerLevel = DebuggerLevel.NONE

    fun level(): DebuggerLevel {
        return level
    }

    fun switch(level: DebuggerLevel): Debugger {
        this.level = level
        return this
    }

    fun info(s: String) = invoke(s) { h.info(s) }
    fun debug(s: String) = invoke(s) { h.debug(s) }
    fun trace(s: String) = invoke(s) { h.trace(s) }
    fun error(s: String) = h.error(s)

    private fun invoke(s: String, honlai: () -> Unit) {
        when (level) {
            DebuggerLevel.NONE -> honlai.invoke()
            DebuggerLevel.INFO -> h.info(s)
            DebuggerLevel.DEBUG -> h.debug(s)
            DebuggerLevel.TRACE -> h.trace(s)
        }
    }

    fun info(s: String, vararg args: Any?) = invoke(s, { h.info(s, *args) }, *args)
    fun debug(s: String, vararg args: Any?) = invoke(s, { h.debug(s, *args) }, *args)
    fun trace(s: String, vararg args: Any?) = invoke(s, { h.trace(s, *args) }, *args)
    fun error(s: String, vararg args: Any?) = h.error(s, *args)

    private fun invoke(s: String, honlai: () -> Unit, vararg args: Any?) {
        when (level) {
            DebuggerLevel.NONE -> honlai.invoke()
            DebuggerLevel.INFO -> h.info(s, *args)
            DebuggerLevel.DEBUG -> h.debug(s, *args)
            DebuggerLevel.TRACE -> h.trace(s, *args)
        }
    }
}
