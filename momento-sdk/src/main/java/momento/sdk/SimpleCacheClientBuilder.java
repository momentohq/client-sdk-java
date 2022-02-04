package momento.sdk;

import java.time.Duration;
import java.util.Optional;
import momento.sdk.exceptions.InvalidArgumentException;

/** Builder for {@link momento.sdk.SimpleCacheClient} */
public final class SimpleCacheClientBuilder {

  private final String authToken;
  private final int itemDefaultTtlSeconds;
  private Optional<Duration> requestTimeout = Optional.empty();

  SimpleCacheClientBuilder(String authToken, int itemTtlDefaultSeconds) {
    this.authToken = authToken;
    ValidationUtils.ensureValidTtl(itemTtlDefaultSeconds);
    this.itemDefaultTtlSeconds = itemTtlDefaultSeconds;
  }

  public SimpleCacheClientBuilder requestTimeout(Duration requestTimeout) {
    if (requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()) {
      throw new InvalidArgumentException("Request timeout should be positive");
    }
    this.requestTimeout = Optional.of(requestTimeout);
    return this;
  }

  public SimpleCacheClient build() {
    return new SimpleCacheClient(
        authToken, itemDefaultTtlSeconds, Optional.empty(), requestTimeout);
  }
}
