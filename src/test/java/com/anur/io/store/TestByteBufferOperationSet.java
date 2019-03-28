package com.anur.io.store;

import java.util.Iterator;
import com.anur.core.command.core.Operation;
import com.anur.io.store.common.OperationAndOffset;
import com.anur.core.command.common.OperationTypeEnum;
import com.anur.io.store.operationset.ByteBufferOperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/3/15
 */
public class TestByteBufferOperationSet {

    public static void main(String[] args) {
        testIterator();
    }

    public static void testIterator() {
        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(new Operation(OperationTypeEnum.REGISTER, "zzz", "sss"), 1);

        Iterator<OperationAndOffset> iterator = byteBufferOperationSet.iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next()
                                       .getOperation()
                                       .toString());
        }
    }
}
