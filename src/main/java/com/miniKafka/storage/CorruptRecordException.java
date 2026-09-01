package com.minikafka.storage;

/** Thrown when a record read from disk fails its CRC check (bit-rot or a torn write). */
public class CorruptRecordException extends RuntimeException {
    public CorruptRecordException(String message) {
        super(message);
    }
}