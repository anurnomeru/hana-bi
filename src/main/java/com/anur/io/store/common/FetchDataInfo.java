package com.anur.io.store.common;

import com.anur.io.store.log.LogOffsetMetadata;
import com.anur.io.store.operationset.FileOperationSet;
import com.anur.io.store.operationset.OperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/2/28
 *
 * 在某个日志文件中读取操作日志时用到
 */
public class FetchDataInfo {

    private LogOffsetMetadata fetchOffsetMetadata;

    private FileOperationSet fileOperationSet;

    public FetchDataInfo(LogOffsetMetadata fetchOffsetMetadata, FileOperationSet fileOperationSet) {
        this.fetchOffsetMetadata = fetchOffsetMetadata;
        this.fileOperationSet = fileOperationSet;
    }

    public LogOffsetMetadata getFetchOffsetMetadata() {
        return fetchOffsetMetadata;
    }

    public FileOperationSet getFileOperationSet() {
        return fileOperationSet;
    }
}
