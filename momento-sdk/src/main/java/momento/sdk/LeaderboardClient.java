package momento.sdk;

import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfiguration;

public class LeaderboardClient implements AutoCloseable {

  private final LeaderboardDataClient dataClient;

  /**
   * Constructs a LeaderboardClient.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   */
  public LeaderboardClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull LeaderboardConfiguration configuration) {
    this.dataClient = new LeaderboardDataClient(credentialProvider, configuration);
  }

  /**
   * Creates a LeaderboardClient builder.
   *
   * @param credentialProvider Provider for the credentials required to connect to Momento.
   * @param configuration Configuration object containing all tunable client settings.
   * @return The builder.
   */
  public static LeaderboardClientBuilder builder(
      CredentialProvider credentialProvider, LeaderboardConfiguration configuration) {
    return new LeaderboardClientBuilder(credentialProvider, configuration);
  }

  /** Creates a leaderboard instance with the given cache and leaderboard names. */
  public ILeaderboard leaderboard(@Nonnull String cacheName, @Nonnull String leaderboardName) {
    return new Leaderboard(this.dataClient, cacheName, leaderboardName);
  }

  @Override
  public void close() {
    this.dataClient.close();
  }
}
