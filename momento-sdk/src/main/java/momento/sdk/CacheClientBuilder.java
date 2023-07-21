package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.DefaultRetryEligibilityStrategy;
import momento.sdk.retry.FixedCountRetryStrategy;
import momento.sdk.retry.RetryEligibilityStrategy;
import momento.sdk.retry.RetryStrategy;

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
   * Sets the maximum duration of a client call.
   *
   * @param deadline The deadline duration.
   * @return The updated builder.
   */
  public CacheClientBuilder setDeadline(@Nonnull Duration deadline) {
    ValidationUtils.ensureRequestDeadlineValid(deadline);

    final GrpcConfiguration newGrpcConfiguration =
        configuration.getTransportStrategy().getGrpcConfiguration().withDeadline(deadline);
    final TransportStrategy newTransportStrategy =
        configuration.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    final RetryStrategy retryStrategy =
        configuration.getRetryStrategy() == null
            ?
            // create default if the configuration didn't have a retry strategy otherwise don't
            // override a customer provided strategy
            new FixedCountRetryStrategy(Configuration.MAX_RETRIES)
            : configuration.getRetryStrategy();
    final RetryEligibilityStrategy retryEligibilityStrategy =
        configuration.getRetryEligibilityStrategy() == null
            ?
            // create default if the configuration didn't have a retry eligbility strategy otherwise
            // don't override a customer provided strategy
            new DefaultRetryEligibilityStrategy()
            : configuration.getRetryEligibilityStrategy();
    configuration =
        configuration.withTransportStrategy(
            newTransportStrategy, retryStrategy, retryEligibilityStrategy);

    return this;
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
