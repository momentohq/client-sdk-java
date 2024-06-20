package momento.sdk.config;

import momento.sdk.config.transport.TransportStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class StorageConfiguration {

  private final TransportStrategy transportStrategy;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   */
  public StorageConfiguration(TransportStrategy transportStrategy) {
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
   * Copy constructor that modifies the transport strategy.
   *
   * @param transportStrategy
   * @return a new Configuration with the updated transport strategy
   */
  public StorageConfiguration withTransportStrategy(final TransportStrategy transportStrategy) {
    return new StorageConfiguration(transportStrategy);
  }
}
