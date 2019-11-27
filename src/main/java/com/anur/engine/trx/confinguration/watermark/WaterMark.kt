package com.anur.engine.trx.confinguration.watermark


/**
 * Created by Anur IjuoKaruKas on 2019/11/27
 *
 * 记载创建时低水位，高水位与未提交的活跃事务
 */
class WaterMark(val lowMark: Long, val heightMark: Long, val unCommittedTrx: HashSet<Long>)