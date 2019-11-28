package com.anur.engine.trx.watermark


/**
 * Created by Anur IjuoKaruKas on 2019/11/28
 *
 * releaseSegment 是否释放了整个段，如果释放了整个段，可以释放这个段的锁
 *
 * releaseLowWaterMark 是否释放了最低的水位，如果释放了，可以告知commitPart不必再持有多版本快照了，可以将数据推入LSM
 */
class WaterReleaseResult(val releaseSegment: Boolean = false, val releaseLowWaterMark: Boolean = false)