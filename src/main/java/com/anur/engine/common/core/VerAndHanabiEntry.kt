package com.anur.engine.common.core

import com.anur.engine.common.entry.ByteBufferHanabiEntry


/**
 * Created by Anur IjuoKaruKas on 2019/10/12
 */
class VerAndHanabiEntry(val trxId: Long, val hanabiEntry: ByteBufferHanabiEntry, var currentVersion: VerAndHanabiEntry? = null)