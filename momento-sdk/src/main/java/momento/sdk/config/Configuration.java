package momento.sdk.config;

import momento.sdk.config.transport.TransportStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class Configuration {

  private final TransportStrategy transportStrategy;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   */
  public Configuration(TransportStrategy transportStrategy) {
    this.transportStrategy = transportStrategy;
  }

  /**
   * Configuration for network tunables.
   *
   * @return The transport strategy
   */
  public TransportStrategy getTransportStrategy() {
    return transportStrategy;
  }

  /**
   * Creates a new instance of the configuration object updated to use the given transport strategy.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @return A copy of this Configuration using the new transport strategy
   */
  public Configuration withTransportStrategy(TransportStrategy transportStrategy) {
    return new Configuration(transportStrategy);
  }
}
