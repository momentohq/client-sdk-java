package momento.sdk.config;

import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.RetryStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class StorageConfiguration {

  private final TransportStrategy transportStrategy;
  private final RetryStrategy retryStrategy;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   */
  public StorageConfiguration(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = retryStrategy;
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
    return new StorageConfiguration(transportStrategy, this.retryStrategy);
  }

  /**
   * Configuration for retry tunables. By default, {@link momento.sdk.retry.FixedCountRetryStrategy}
   * gets used.
   *
   * @return The retry strategy
   */
  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  /**
   * Copy constructor that modifies the retry strategy.
   *
   * @param retryStrategy
   * @return a new Configuration with the updated retry strategy
   */
  public StorageConfiguration withRetryStrategy(final RetryStrategy retryStrategy) {
    return new StorageConfiguration(this.transportStrategy, retryStrategy);
  }
}
