package com.anur.engine.trx.watermark


/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 记载创建时低水位，高水位与未提交的活跃事务
 * 高水位就是自己
 */
class WaterMarker(val lowMark: Long, val heightMark: Long, val waterHolder: WaterHolder)