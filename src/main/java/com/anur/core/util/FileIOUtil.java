package com.anur.core.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
/**
 * Created by Anur IjuoKaruKas on 2019/2/27
 */
public class FileIOUtil {

    /**
     * 开启一个文件channel
     *
     * mutable，是否可改变（是否不可读）
     * true 可读可写
     * false 只可读
     */
    public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException {
        if (mutable) {
            return new RandomAccessFile(file, "rw").getChannel();
        } else {
            return new FileInputStream(file).getChannel();
        }
    }
}
