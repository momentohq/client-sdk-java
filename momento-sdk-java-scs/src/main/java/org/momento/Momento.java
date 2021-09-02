package org.momento;

import org.momento.scs.ScsClient;

import java.util.Optional;

public final class Momento {

    private final String authToken;
    private final String endpoint;

    // TODO: Hook In Telemetry
    public Momento(String authToken, String endpoint) {
        this.authToken = authToken;
        this.endpoint = endpoint;
    }

    public ScsClient getCache(String cacheName) {
        // TODO: Grpc call to control plane.
        // Also, may be check status?
        String cacheId = "CACHE_ID";
        String cacheEndpoint = "beta.cacheservice.com";

        return new ScsClient(authToken, cacheId, Optional.empty(), cacheEndpoint);
    }

}
