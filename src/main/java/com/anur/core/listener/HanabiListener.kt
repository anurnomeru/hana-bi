package com.anur.core.listener

import java.util.function.BiFunction

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 */
object HanabiListener {

    private val EVENT: MutableMap<EventEnum, Registion> = mutableMapOf()

    @Synchronized
    fun register(event: EventEnum, action: () -> Any?) {
        EVENT.compute(event, BiFunction { _, v ->
            var registion = v
            if (registion == null) {
                registion = Registion()
            }
            registion.register(action)
            return@BiFunction registion
        })
    }

    fun onEvent(event: EventEnum) {
        EVENT.get(event)?.onEvent()
    }

    class Registion {
        private val actionRegister: MutableList<() -> Any?> = mutableListOf()

        fun register(action: () -> Any?) {
            actionRegister.add(action)
        }

        fun onEvent() {
            actionRegister.forEach { function: () -> Any? -> function.invoke() }
        }
    }
}