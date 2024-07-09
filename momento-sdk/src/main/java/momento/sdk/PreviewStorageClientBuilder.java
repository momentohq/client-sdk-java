package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.StorageConfiguration;
import momento.sdk.config.StorageConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder for {@link PreviewStorageClient} */
public final class PreviewStorageClientBuilder {
  private final Logger logger = LoggerFactory.getLogger(PreviewStorageClient.class);
  private CredentialProvider credentialProvider;
  private StorageConfiguration configuration;

  /** Creates a PreviewStorageClient builder. */
  PreviewStorageClientBuilder() {
    this.credentialProvider = null;
    this.configuration = StorageConfigurations.Laptop.latest();
  }

  /**
   * Sets the credential provider.
   *
   * @param credentialProvider the provider.
   * @return this builder.
   */
  public PreviewStorageClientBuilder withCredentialProvider(
      @Nonnull CredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
    return this;
  }

  /**
   * Sets the configuration.
   *
   * @param configuration the configuration.
   * @return this builder.
   */
  public PreviewStorageClientBuilder withConfiguration(
      @Nonnull StorageConfiguration configuration) {
    this.configuration = configuration;
    return this;
  }

  /**
   * Builds a PreviewStorageClient.
   *
   * @return the client.
   */
  public PreviewStorageClient build() {
    if (credentialProvider == null) {
      credentialProvider = new EnvVarCredentialProvider("MOMENTO_API_KEY");
    }

    if (configuration != null && configuration instanceof StorageConfigurations.Laptop) {
      logger.warn(
          "Using the Laptop configuration for the PreviewStorageClient. This is not recommended for production use.");
    }

    return new PreviewStorageClient(credentialProvider, configuration);
  }
}
