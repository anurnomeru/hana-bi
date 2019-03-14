package com.anur.core.store.common;

import com.anur.core.store.log.LogOffsetMetadata;
import com.anur.core.store.operationset.OperationSet;

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

    public LogOffsetMetadata getFetchOffsetMetadata() {
        return fetchOffsetMetadata;
    }

    public OperationSet getOperationSet() {
        return operationSet;
    }
}
