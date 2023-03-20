package momento.sdk;

import java.time.Duration;
import java.util.Optional;

/** Builder for {@link momento.sdk.SimpleCacheClient} */
public final class SimpleCacheClientBuilder {

  private final String authToken;
  private final long itemDefaultTtlSeconds;
  private Optional<Duration> requestTimeout = Optional.empty();

  SimpleCacheClientBuilder(String authToken, long itemTtlDefaultSeconds) {
    this.authToken = authToken;
    ValidationUtils.ensureValidTtl(itemTtlDefaultSeconds);
    this.itemDefaultTtlSeconds = itemTtlDefaultSeconds;
  }

  public SimpleCacheClientBuilder requestTimeout(Duration requestTimeout) {
    ValidationUtils.ensureRequestTimeoutValid(requestTimeout);
    this.requestTimeout = Optional.of(requestTimeout);
    return this;
  }

  public SimpleCacheClient build() {
    return new SimpleCacheClient(
        authToken, itemDefaultTtlSeconds, Optional.empty(), requestTimeout);
  }
}
