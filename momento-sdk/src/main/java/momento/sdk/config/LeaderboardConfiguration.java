package momento.sdk.config;

import momento.sdk.config.transport.leaderboard.LeaderboardTransportStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class LeaderboardConfiguration {

  private final LeaderboardTransportStrategy transportStrategy;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   */
  public LeaderboardConfiguration(LeaderboardTransportStrategy transportStrategy) {
    this.transportStrategy = transportStrategy;
  }

  /**
   * Configuration for network tunables.
   *
   * @return The transport strategy
   */
  public LeaderboardTransportStrategy getTransportStrategy() {
    return transportStrategy;
  }

  /**
   * Copy constructor that modifies the transport strategy.
   *
   * @param transportStrategy the new transport strategy
   * @return a new Configuration with the updated transport strategy
   */
  public LeaderboardConfiguration withTransportStrategy(
      final LeaderboardTransportStrategy transportStrategy) {
    return new LeaderboardConfiguration(transportStrategy);
  }
}
