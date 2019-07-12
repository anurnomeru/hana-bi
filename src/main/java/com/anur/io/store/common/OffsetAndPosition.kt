package com.anur.io.store.common

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 记录了一条操作记录的 逻辑上的 offset 信息，与物理上的 position 信息
 */
class OffsetAndPosition(

    /**
     * offset 代表逻辑上的文件位置
     */
    val offset: Long,

    /**
     * position 代表物理上的位置
     */
    val position: Int)