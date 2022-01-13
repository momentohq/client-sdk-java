package momento.sdk;

import java.util.Optional;
import momento.sdk.exceptions.ValidationException;

/** Builder for {@link momento.sdk.SimpleCacheClient} */
public final class SimpleCacheClientBuilder {

  private final String authToken;
  private final int itemDefaultTtlSeconds;

  SimpleCacheClientBuilder(String authToken, int itemTtlDefaultSeconds) {
    this.authToken = authToken;
    this.itemDefaultTtlSeconds = itemTtlDefaultSeconds;
  }

  public SimpleCacheClient build() {
    if (itemDefaultTtlSeconds < 0) {
      throw new ValidationException("Item's time to live in Cache cannot be negative.");
    }
    return new SimpleCacheClient(authToken, itemDefaultTtlSeconds, Optional.empty());
  }
}
