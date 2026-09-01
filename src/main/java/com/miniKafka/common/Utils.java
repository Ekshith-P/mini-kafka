package com.minikafka.common;

/** Small shared helpers: the key partitioner and a couple of bit tricks. */
public final class Utils {

    private Utils() {
    }

    /** Clears the sign bit so a hash maps cleanly onto {@code [0, numPartitions)}. */
    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    /**
     * Picks a partition for a record. When a key is present we hash it (murmur2, same algorithm
     * Kafka's default partitioner uses) so all records with the same key land on the same
     * partition and stay ordered. When there is no key the caller should round-robin instead.
     */
    public static int partitionForKey(byte[] key, int numPartitions) {
        return toPositive(murmur2(key)) % numPartitions;
    }

    /**
     * Murmur2 hash, byte-for-byte compatible with Apache Kafka's
     * {@code org.apache.kafka.common.utils.Utils#murmur2}. Reimplemented here so we depend only
     * on the JDK.
     */
    public static int murmur2(final byte[] data) {
        final int length = data.length;
        final int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        final int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4] & 0xff)
                    + ((data[i4 + 1] & 0xff) << 8)
                    + ((data[i4 + 2] & 0xff) << 16)
                    + ((data[i4 + 3] & 0xff) << 24);

            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
                // fall through
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
                // fall through
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
                break;
            default:
                break;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}