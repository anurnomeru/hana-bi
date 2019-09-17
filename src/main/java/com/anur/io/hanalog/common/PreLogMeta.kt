package com.anur.io.hanalog.common

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 预日志的内容
 */
class PreLogMeta(val startOffset: Long, val endOffset: Long, val oao: Collection<OperationAndOffset>)