package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;

/** Builder for {@link CacheClient} */
public final class CacheClientBuilder {

  private final String authToken;
  private final Duration itemDefaultTtl;
  private Duration requestTimeout = null;

  CacheClientBuilder(@Nonnull String authToken, @Nonnull Duration itemDefaultTtl) {
    this.authToken = authToken;
    ValidationUtils.ensureValidTtl(itemDefaultTtl);
    this.itemDefaultTtl = itemDefaultTtl;
  }

  public CacheClientBuilder requestTimeout(@Nonnull Duration requestTimeout) {
    ValidationUtils.ensureRequestTimeoutValid(requestTimeout);
    this.requestTimeout = requestTimeout;
    return this;
  }

  public CacheClient build() {
    return new CacheClient(authToken, itemDefaultTtl, null, requestTimeout);
  }
}
