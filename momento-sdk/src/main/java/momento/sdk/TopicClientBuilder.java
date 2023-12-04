package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;

/** Builder for {@link TopicClient} */
public final class TopicClientBuilder {

  private final CredentialProvider credentialProvider;
  private Configuration configuration;

  /**
   * Creates a TopicClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   */
  TopicClientBuilder(
      @Nonnull CredentialProvider credentialProvider, @Nonnull Configuration configuration) {
    this.credentialProvider = credentialProvider;
    this.configuration = configuration;
  }

  /**
   * Builds a TopicClient.
   *
   * @return the client.
   */
  public TopicClient build() {
    return new TopicClient(credentialProvider, configuration);
  }
}
