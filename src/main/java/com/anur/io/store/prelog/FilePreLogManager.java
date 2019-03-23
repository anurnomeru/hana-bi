package com.anur.io.store.prelog;

import java.io.File;
import com.anur.config.InetSocketAddressConfigHelper;
import com.anur.io.store.manager.LogManager;

/**
 * Created by Anur IjuoKaruKas on 2019/3/22
 */
public class FilePreLogManager extends LogManager {

    public static volatile FilePreLogManager INSTANCE;

    public static LogManager getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (LogManager.class) {
                if (INSTANCE == null) {
                    String relativelyPath = System.getProperty("user.dir");
                    INSTANCE = new FilePreLogManager(new File(relativelyPath + "\\" + InetSocketAddressConfigHelper.getServerName() + "\\prelog\\aof\\"));
                }
            }
        }
        return INSTANCE;
    }

    public FilePreLogManager(File path) {
        super(path);
    }
}
