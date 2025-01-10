package momento.sdk.config.transport.leaderboard;

/**
 * The simplest way to configure gRPC for the Momento leaderboard client. Specifies static values
 * the transport options.
 */
public class StaticLeaderboardTransportStrategy implements LeaderboardTransportStrategy {

  private final LeaderboardGrpcConfiguration grpcConfiguration;

  /**
   * Constructs a StaticStorageTransportStrategy.
   *
   * @param grpcConfiguration gRPC tunables.
   */
  public StaticLeaderboardTransportStrategy(LeaderboardGrpcConfiguration grpcConfiguration) {
    this.grpcConfiguration = grpcConfiguration;
  }

  @Override
  public LeaderboardGrpcConfiguration getGrpcConfiguration() {
    return grpcConfiguration;
  }

  @Override
  public LeaderboardTransportStrategy withGrpcConfiguration(
      LeaderboardGrpcConfiguration grpcConfiguration) {
    return new StaticLeaderboardTransportStrategy(grpcConfiguration);
  }
}
