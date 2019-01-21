package com.anur.exception;

/**
 * Created by Anur IjuoKaruKas on 2019/1/21
 */
public class HanabiException extends RuntimeException {

    public HanabiException() {
        super();
    }

    public HanabiException(String message) {
        super(message);
    }

    public HanabiException(String message, Throwable cause) {
        super(message, cause);
    }

    public HanabiException(Throwable cause) {
        super(cause);
    }

    protected HanabiException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
