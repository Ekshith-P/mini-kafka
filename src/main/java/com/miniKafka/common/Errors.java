package com.minikafka.common;

/**
 * Error codes returned inside response bodies (never in the response header, exactly like Kafka).
 *
 * <p>A code of {@link #NONE} (0) means success. Negative/positive codes map to specific failure
 * conditions so a client can react programmatically (e.g. refresh metadata on
 * {@link #NOT_LEADER_FOR_PARTITION}).
 */
public final class Errors {
    public static final short NONE = 0;
    public static final short UNKNOWN = -1;
    public static final short OFFSET_OUT_OF_RANGE = 1;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short NOT_LEADER_FOR_PARTITION = 6;
    public static final short REQUEST_TIMED_OUT = 7;
    public static final short ILLEGAL_GENERATION = 22;
    public static final short UNKNOWN_MEMBER_ID = 25;
    public static final short REBALANCE_IN_PROGRESS = 27;
    public static final short UNSUPPORTED_VERSION = 35;
    public static final short TOPIC_ALREADY_EXISTS = 36;
    public static final short INVALID_REQUEST = 42;

    private Errors() {
    }

    public static String message(short code) {
        switch (code) {
            case NONE:
                return "NONE";
            case OFFSET_OUT_OF_RANGE:
                return "OFFSET_OUT_OF_RANGE";
            case UNKNOWN_TOPIC_OR_PARTITION:
                return "UNKNOWN_TOPIC_OR_PARTITION";
            case NOT_LEADER_FOR_PARTITION:
                return "NOT_LEADER_FOR_PARTITION";
            case REQUEST_TIMED_OUT:
                return "REQUEST_TIMED_OUT";
            case ILLEGAL_GENERATION:
                return "ILLEGAL_GENERATION";
            case UNKNOWN_MEMBER_ID:
                return "UNKNOWN_MEMBER_ID";
            case REBALANCE_IN_PROGRESS:
                return "REBALANCE_IN_PROGRESS";
            case UNSUPPORTED_VERSION:
                return "UNSUPPORTED_VERSION";
            case TOPIC_ALREADY_EXISTS:
                return "TOPIC_ALREADY_EXISTS";
            case INVALID_REQUEST:
                return "INVALID_REQUEST";
            default:
                return "UNKNOWN";
        }
    }
}