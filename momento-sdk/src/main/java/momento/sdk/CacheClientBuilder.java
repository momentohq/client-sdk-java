package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;

/** Builder for {@link CacheClient} */
public final class CacheClientBuilder {

  private final CredentialProvider credentialProvider;
  private Configuration configuration;
  private final Duration itemDefaultTtl;

  /**
   * Creates a CacheClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @param itemDefaultTtl The default ttl for values written to a cache.
   */
  CacheClientBuilder(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration itemDefaultTtl) {
    this.credentialProvider = credentialProvider;
    this.configuration = configuration;
    ValidationUtils.ensureValidTtl(itemDefaultTtl);
    this.itemDefaultTtl = itemDefaultTtl;
  }

  /**
   * Builds a CacheClient.
   *
   * @return the client.
   */
  public CacheClient build() {
    return new CacheClient(credentialProvider, configuration, itemDefaultTtl);
  }
}
