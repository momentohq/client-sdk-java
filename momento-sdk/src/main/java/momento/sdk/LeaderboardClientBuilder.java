package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfiguration;

/** Builder for {@link LeaderboardClient} */
public final class LeaderboardClientBuilder {

  private final CredentialProvider credentialProvider;
  private LeaderboardConfiguration configuration;

  /**
   * Creates a LeaderboardClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   */
  LeaderboardClientBuilder(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull LeaderboardConfiguration configuration) {
    this.credentialProvider = credentialProvider;
    this.configuration = configuration;
  }

  /**
   * Builds a LeaderboardClient.
   *
   * @return the client.
   */
  public LeaderboardClient build() {
    return new LeaderboardClient(credentialProvider, configuration);
  }
}
