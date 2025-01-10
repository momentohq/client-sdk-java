package momento.sdk.config.transport.leaderboard;

/** Configuration for network tunables. */
public interface LeaderboardTransportStrategy {
  /**
   * Configures the low-level gRPC settings for the client's communication with the Momento server.
   *
   * @return the gRPC configuration.
   */
  LeaderboardGrpcConfiguration getGrpcConfiguration();

  /**
   * Copy constructor that modifies the gRPC configuration.
   *
   * @param grpcConfiguration low-level gRPC settings.
   * @return The modified StorageTransportStrategy.
   */
  LeaderboardTransportStrategy withGrpcConfiguration(
      LeaderboardGrpcConfiguration grpcConfiguration);
}
