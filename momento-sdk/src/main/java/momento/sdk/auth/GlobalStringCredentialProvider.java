package momento.sdk.auth;

import javax.annotation.Nonnull;
import momento.sdk.exceptions.InvalidArgumentException;

public class GlobalStringCredentialProvider extends CredentialProvider {
    private static String build(String prefix, String endpoint) {
        return prefix + "." + endpoint;
    }

    public GlobalStringCredentialProvider(@Nonnull String authToken, @Nonnull String endpoint) {
        if (authToken == null) {
            throw new InvalidArgumentException("Auth token must not be empty");
        }
        if (endpoint == null) {
            throw new InvalidArgumentException("Endpoint must not be empty");
        }
        this.authToken = authToken;
        this.controlEndpoint = build("control", endpoint);
        this.cacheEndpoint = build("cache", endpoint);
        this.storageEndpoint = build("storage", endpoint);
        this.tokenEndpoint = build("token", endpoint);

    }

    @Override
    public String getAuthToken() {
        return authToken;
    }

    @Override
    public String getControlEndpoint() {
        return controlEndpoint;
    }

    @Override
    public String getCacheEndpoint() {
        return cacheEndpoint;
    }

    @Override
    public String getStorageEndpoint() {
        return storageEndpoint;
    }

    @Override
    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    @Override
    public boolean isEndpointSecure() {
        return true;
    }
}