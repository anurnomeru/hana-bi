package com.anur.core.command.coordinate;

import com.anur.io.store.common.Operation;
import com.anur.io.store.common.OperationTypeEnum;

/**
 * Created by Anur IjuoKaruKas on 2019/3/27
 */
public class Register extends Operation {

    public Register(String serverName) {
        super(OperationTypeEnum.REGISTER, serverName, "");
    }

    public String getServerName() {
        return this.getKey();
    }
}
