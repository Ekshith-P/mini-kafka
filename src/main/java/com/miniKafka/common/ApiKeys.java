package com.minikafka.common;

/**
 * Numeric identifiers for each request type on the wire.
 *
 * <p>These deliberately mirror Apache Kafka's real protocol API keys so the design maps 1:1 to
 * how production Kafka frames requests. Every request begins with a 2-byte api key that tells the
 * broker how to parse the rest of the payload.
 */
public enum ApiKeys {
    PRODUCE((short) 0),
    FETCH((short) 1),
    LIST_OFFSETS((short) 2),
    METADATA((short) 3),
    OFFSET_COMMIT((short) 8),
    OFFSET_FETCH((short) 9),
    JOIN_GROUP((short) 11),
    HEARTBEAT((short) 12),
    LEAVE_GROUP((short) 13),
    SYNC_GROUP((short) 14),
    API_VERSIONS((short) 18),
    CREATE_TOPICS((short) 19),

    // Internal broker-to-broker APIs (well above the Kafka-mirrored client keys above).
    RAFT_REQUEST_VOTE((short) 200),
    RAFT_APPEND_ENTRIES((short) 201);

    public final short id;

    ApiKeys(short id) {
        this.id = id;
    }

    private static final ApiKeys[] BY_ID = buildIndex();

    private static ApiKeys[] buildIndex() {
        short max = 0;

        for (ApiKeys k : values()) {
            if (k.id > max) {
                max = k.id;
            }
        }

        ApiKeys[] index = new ApiKeys[max + 1];

        for (ApiKeys k : values()) {
            index[k.id] = k;
        }

        return index;
    }

    /** Returns the api key for the given id, or {@code null} if unknown. */
    public static ApiKeys forId(short id) {
        if (id < 0 || id >= BY_ID.length) {
            return null;
        }

        return BY_ID[id];
    }
}