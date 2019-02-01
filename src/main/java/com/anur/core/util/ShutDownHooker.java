package com.anur.core.util;

import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Anur IjuoKaruKas on 1/30/2019
 *
 * 关闭socket连接的钩子，因为连接不上会创建新的socket连接，所以要用一个钩子来建立与最新socket的关联
 */
public class ShutDownHooker {

    private static Logger logger = LoggerFactory.getLogger(ShutDownHooker.class);

    private boolean shutDown;

    private Consumer<Void> shutDownConsumer;

    private String shutDownMsg;

    public ShutDownHooker(String shutDownMsg) {
        this.shutDownMsg = shutDownMsg;
        this.shutDown = false;
        this.shutDownConsumer = aVoid -> {
        };
    }

    public synchronized void shutdown() {
        logger.info(shutDownMsg);
        shutDown = true;
        shutDownConsumer.accept(null);
    }

    public synchronized void shutDownRegister(Consumer<Void> shutDownSupplier) {

        // 如果已经事先触发了关闭，则不需要再注册关闭事件了，直接调用关闭方法
        if (shutDown) {
            shutDownConsumer.accept(null);
        }
        this.shutDownConsumer = shutDownSupplier;
    }

    public synchronized boolean isShutDown() {
        return shutDown;
    }
}
