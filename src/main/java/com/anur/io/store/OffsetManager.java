package com.anur.io.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import com.anur.config.LogConfigHelper;
import com.anur.core.elect.model.GenerationAndOffset;
import com.anur.core.lock.ReentrantReadWriteLocker;

/**
 * Created by Anur IjuoKaruKas on 2019/3/26
 *
 * 仅仅保存提交到了哪里，Leader副本才有这个，因为 Leader 副本的预日志是直接写入日志的
 *
 * 所以如果Leader挂了，需要抹除此 offset 以后的所有日志
 */
public class OffsetManager extends ReentrantReadWriteLocker {

    private final File offsetFile;

    private final MappedByteBuffer mmap;

    private volatile GenerationAndOffset current;

    private static volatile OffsetManager INSTANCE;

    public static OffsetManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (OffsetManager.class) {
                if (INSTANCE == null) {
                    try {
                        INSTANCE = new OffsetManager();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return INSTANCE;
    }

    private OffsetManager() throws IOException {
        this.offsetFile = new File(LogConfigHelper.getBaseDir(), "commitOffset.temp");
        boolean newCreate = offsetFile.createNewFile();

        RandomAccessFile raf = new RandomAccessFile(offsetFile, "rw");
        raf.setLength(8 + 8);

        this.mmap = raf.getChannel()
                       .map(MapMode.READ_WRITE, 0, 8 + 8);

        if (newCreate) {
            current = GenerationAndOffset.INVALID;
        } else {

        }
    }

    public GenerationAndOffset load() {
        if (current == null) {
            this.readLockSupplier(() ->
                {
                    if (current == null) {
                        long gen = mmap.getLong();
                        long offset = mmap.getLong();
                        current = new GenerationAndOffset(gen, offset);
                        mmap.rewind();
                    }
                    return null;
                }
            );
        }
        return current;
    }

    public void cover(GenerationAndOffset GAO) {
        this.writeLockSupplier(() -> {
                current = null;
                mmap.putLong(GAO.getGeneration());
                mmap.putLong(GAO.getOffset());
                return null;
            }
        );
    }
}
