package momento.sdk;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;

/** Builder for {@link CacheClient} */
public final class CacheClientBuilder {

  private final CredentialProvider credentialProvider;
  private Configuration configuration;
  private final Duration itemDefaultTtl;

  CacheClientBuilder(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull Configuration configuration,
      @Nonnull Duration itemDefaultTtl) {
    this.credentialProvider = credentialProvider;
    this.configuration = configuration;
    ValidationUtils.ensureValidTtl(itemDefaultTtl);
    this.itemDefaultTtl = itemDefaultTtl;
  }

  public CacheClientBuilder setDeadline(@Nonnull Duration deadline) {
    ValidationUtils.ensureRequestDeadlineValid(deadline);

    final GrpcConfiguration newGrpcConfiguration =
        configuration.getTransportStrategy().getGrpcConfiguration().withDeadline(deadline);
    final TransportStrategy newTransportStrategy =
        configuration.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    configuration = configuration.withTransportStrategy(newTransportStrategy);

    return this;
  }

  public CacheClient build() {
    return new CacheClient(credentialProvider, configuration, itemDefaultTtl);
  }
}
