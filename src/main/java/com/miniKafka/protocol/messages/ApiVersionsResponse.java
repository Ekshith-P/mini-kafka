package com.minikafka.protocol.messages;

import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * API_VERSIONS response: advertises which request types this broker supports and the version range
 * for each. A client sends an (empty-bodied) ApiVersions request on connect to negotiate what it can
 * use - the same handshake Kafka performs.
 */
public final class ApiVersionsResponse {

    public static final class ApiVersion {
        public final short apiKey;
        public final short minVersion;
        public final short maxVersion;

        public ApiVersion(short apiKey, short minVersion, short maxVersion) {
            this.apiKey = apiKey;
            this.minVersion = minVersion;
            this.maxVersion = maxVersion;
        }
    }

    public final short errorCode;
    public final List<ApiVersion> apiVersions;

    public ApiVersionsResponse(short errorCode, List<ApiVersion> apiVersions) {
        this.errorCode = errorCode;
        this.apiVersions = apiVersions;
    }

    /** Builds a response advertising v0 support for every API this broker implements. */
    public static ApiVersionsResponse supported() {
        List<ApiVersion> versions = new ArrayList<>();
        for (ApiKeys key : ApiKeys.values()) {
            versions.add(new ApiVersion(key.id, (short) 0, (short) 0));
        }
        return new ApiVersionsResponse(Errors.NONE, versions);
    }

    public void writeTo(ByteWriter out) {
        out.putShort(errorCode);
        out.putInt(apiVersions.size());
        for (ApiVersion v : apiVersions) {
            out.putShort(v.apiKey);
            out.putShort(v.minVersion);
            out.putShort(v.maxVersion);
        }
    }

    public static ApiVersionsResponse parse(ByteBuffer buf) {
        short errorCode = buf.getShort();
        int count = buf.getInt();
        List<ApiVersion> versions = new ArrayList<>(Math.max(0, count));
        for (int i = 0; i < count; i++) {
            short apiKey = buf.getShort();
            short minVersion = buf.getShort();
            short maxVersion = buf.getShort();
            versions.add(new ApiVersion(apiKey, minVersion, maxVersion));
        }
        return new ApiVersionsResponse(errorCode, versions);
    }
}