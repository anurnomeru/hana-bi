package com.anur.core.log.common;

import com.anur.core.log.LogOffsetMetadata;
import com.anur.core.log.operationset.OperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/2/28
 *
 * 在某个日志文件中读取操作日志时用到
 */
public class FetchDataInfo {

    private LogOffsetMetadata fetchOffsetMetadata;

    private OperationSet operationSet;

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata, OperationSet operationSet) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.operationSet = operationSet;
    }
}
