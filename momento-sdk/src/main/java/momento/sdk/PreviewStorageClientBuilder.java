package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.StorageConfiguration;
import momento.sdk.config.StorageConfigurations;

/** Builder for {@link PreviewStorageClient} */
public final class PreviewStorageClientBuilder {
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
  public IPreviewStorageClient build() {
    if (credentialProvider == null) {
      credentialProvider = new EnvVarCredentialProvider("MOMENTO_API_KEY");
    }
    return new PreviewStorageClient(credentialProvider, configuration);
  }
}
