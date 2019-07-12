package com.anur.io.store.common

import com.anur.io.store.log.LogOffsetMetadata
import com.anur.io.store.operationset.FileOperationSet

/**
 * Created by Anur IjuoKaruKas on 2019/7/12
 *
 * 在某个日志文件中读取操作日志时用到，
 * 读取某个 GAO 时，会用此类包装结果
 */
class FetchDataInfo(

    /**
     *  A log offset structure, including:
     * 1. the generation
     * 2. the message offset
     * 3. the base message offset of the located segment
     * 4. the physical position on the located segment
     */
    val fetchMeta: LogOffsetMetadata,

    /**
     * the GAO file in
     */
    val fos: FileOperationSet)