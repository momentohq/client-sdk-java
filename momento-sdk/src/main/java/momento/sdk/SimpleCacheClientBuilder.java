package momento.sdk;

import java.util.Optional;

/** Builder for {@link momento.sdk.SimpleCacheClient} */
public final class SimpleCacheClientBuilder {

  private final String authToken;
  private final int itemDefaultTtlSeconds;

  SimpleCacheClientBuilder(String authToken, int itemTtlDefaultSeconds) {
    this.authToken = authToken;
    ValidationUtils.ensureValidTtl(itemTtlDefaultSeconds);
    this.itemDefaultTtlSeconds = itemTtlDefaultSeconds;
  }

  public SimpleCacheClient build() {
    return new SimpleCacheClient(authToken, itemDefaultTtlSeconds, Optional.empty());
  }
}
