package com.anur.core.log.common;

/**
 * Created by Anur IjuoKaruKas on 2019/3/11
 */
public enum OperationTypeEnum {
    SETNX(0);

    public int byteSign;

    OperationTypeEnum(int byteSign) {
        this.byteSign = byteSign;
    }}
