package com.anur.io.hanalog;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.anur.core.struct.base.Operation;
import com.anur.io.hanalog.common.OperationAndOffset;
import com.anur.core.struct.OperationTypeEnum;
import com.anur.io.hanalog.operationset.ByteBufferOperationSet;

/**
 * Created by Anur IjuoKaruKas on 2019/3/15
 */
public class TestByteBufferOperationSet {

    public static void main(String[] args) {
    }

//    public static void testIterator() {
//        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(new Operation(OperationTypeEnum.REGISTER, "zzz", "sss".getBytes(Charset.defaultCharset())), 1);
//
//        Iterator<OperationAndOffset> iterator = byteBufferOperationSet.iterator();
//
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next()
//                                       .getOperation()
//                                       .toString());
//        }
//    }
//
//    public static void testIterator1() {
//        List<OperationAndOffset> operations = new ArrayList<>();
//        for (int i = 0; i < 100; i++) {
//            Operation operation = new Operation(OperationTypeEnum.SETNX, "123", "2342342352345".getBytes(Charset.defaultCharset()));
//
//            operations.add(new OperationAndOffset(operation, i));
//        }
//
//        ByteBufferOperationSet byteBufferOperationSet = new ByteBufferOperationSet(operations);
//
//        ByteBufferOperationSet byteBufferOperationSet1 = new ByteBufferOperationSet(byteBufferOperationSet.getByteBuffer());
//
//        Iterator<OperationAndOffset> iterator = byteBufferOperationSet1.iterator();
//        while (iterator.hasNext()) {
//            System.out.println(iterator.next()
//                                       .getOffset());
//        }
//    }
}
