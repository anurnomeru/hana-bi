package com.anur.core.util;

import java.util.function.Supplier;
/**
 * Created by Anur IjuoKaruKas on 1/30/2019
 */
public class ShutDownHocker {

    private boolean shutDown;

    private Supplier<Void> shutDownSupplier;

    public synchronized void shutdown() {
        shutDown = true;
        shutDownSupplier.get();
    }

    public void registShutDown(Supplier<Void> shutDownSupplier) {
        this.shutDownSupplier = shutDownSupplier;
    }

    public synchronized boolean isShutDown() {
        return shutDown;
    }
}
