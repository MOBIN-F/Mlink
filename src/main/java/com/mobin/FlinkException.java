package com.mobin;

/**
 * Custom exception class for Flink related errors.
 */
public class FlinkException extends RuntimeException{
    private static final long serialVersionUID = 1L;

    public FlinkException(String message) {
        super(message);
    }

    public FlinkException(String message, Throwable e) {
        super(message, e);
    }
}
