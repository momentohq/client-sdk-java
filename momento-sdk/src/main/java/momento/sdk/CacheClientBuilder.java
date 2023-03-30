package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;

/** Builder for {@link CacheClient} */
public final class CacheClientBuilder {

  private final CredentialProvider credentialProvider;
  private final Duration itemDefaultTtl;
  private Duration requestTimeout = null;

  CacheClientBuilder(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Duration itemDefaultTtl) {
    this.credentialProvider = credentialProvider;
    ValidationUtils.ensureValidTtl(itemDefaultTtl);
    this.itemDefaultTtl = itemDefaultTtl;
  }

  public CacheClientBuilder requestTimeout(@Nonnull Duration requestTimeout) {
    ValidationUtils.ensureRequestTimeoutValid(requestTimeout);
    this.requestTimeout = requestTimeout;
    return this;
  }

  public CacheClient build() {
    return new CacheClient(credentialProvider, itemDefaultTtl, requestTimeout);
  }
}
