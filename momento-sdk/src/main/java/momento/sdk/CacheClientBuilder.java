package momento.sdk;

import java.time.Duration;
import java.util.Optional;

/** Builder for {@link CacheClient} */
public final class CacheClientBuilder {

  private final String authToken;
  private final Duration itemDefaultTtl;
  private Optional<Duration> requestTimeout = Optional.empty();

  CacheClientBuilder(String authToken, Duration itemDefaultTtl) {
    this.authToken = authToken;
    ValidationUtils.ensureValidTtl(itemDefaultTtl);
    this.itemDefaultTtl = itemDefaultTtl;
  }

  public CacheClientBuilder requestTimeout(Duration requestTimeout) {
    ValidationUtils.ensureRequestTimeoutValid(requestTimeout);
    this.requestTimeout = Optional.of(requestTimeout);
    return this;
  }

  public CacheClient build() {
    return new CacheClient(authToken, itemDefaultTtl, Optional.empty(), requestTimeout);
  }
}
