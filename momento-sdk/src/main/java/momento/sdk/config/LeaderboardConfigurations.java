package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.leaderboard.LeaderboardGrpcConfiguration;
import momento.sdk.config.transport.leaderboard.LeaderboardTransportStrategy;
import momento.sdk.config.transport.leaderboard.StaticLeaderboardTransportStrategy;

/** Prebuilt {@link LeaderboardConfiguration}s for different environments. */
public class LeaderboardConfigurations {
  /**
   * Provides defaults suitable for a medium-to-high-latency dev environment. Permissive timeouts,
   * retries, and relaxed latency and throughput targets.
   */
  public static class Laptop extends LeaderboardConfiguration {

    private Laptop(LeaderboardTransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for a dev environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest Laptop configuration
     */
    public static LeaderboardConfiguration latest() {
      final LeaderboardTransportStrategy transportStrategy =
          new StaticLeaderboardTransportStrategy(
              new LeaderboardGrpcConfiguration(Duration.ofMillis(15000)));
      return new Laptop(transportStrategy);
    }
  }

  /**
   * Provides defaults suitable for an environment where your client is running in the same region
   * as the Momento service. It has more aggressive timeouts and retry behavior than the Laptop
   * config.
   */
  public static class InRegion extends LeaderboardConfiguration {

    private InRegion(LeaderboardTransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for an in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest in-region configuration
     */
    public static LeaderboardConfiguration latest() {
      final LeaderboardTransportStrategy transportStrategy =
          new StaticLeaderboardTransportStrategy(
              new LeaderboardGrpcConfiguration(Duration.ofMillis(1100)));
      return new InRegion(transportStrategy);
    }
  }

  public static class Lambda extends LeaderboardConfiguration {
    private Lambda(LeaderboardTransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for a Lambda environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
     * periods of time when the connection is idle. However, they are very problematic for lambda
     * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
     * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
     * timeout even though the connection is completely healthy. Therefore, keep-alives should be
     * disabled in lambda and similar environments.
     *
     * @return the latest Lambda configuration
     */
    public static LeaderboardConfiguration latest() {
      final LeaderboardGrpcConfiguration grpcConfig =
          new LeaderboardGrpcConfiguration(Duration.ofMillis(1100)).withKeepAliveDisabled();
      final LeaderboardTransportStrategy transportStrategy =
          new StaticLeaderboardTransportStrategy(grpcConfig);
      return new Lambda(transportStrategy);
    }
  }
}
