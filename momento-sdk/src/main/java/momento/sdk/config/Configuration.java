package momento.sdk.config;

import java.time.Duration;
import java.util.Optional;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.RetryStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class Configuration {

  private final TransportStrategy transportStrategy;
  private final RetryStrategy retryStrategy;

  private final Integer concurrencyLimit;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   */
  public Configuration(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = retryStrategy;
    this.concurrencyLimit = null;
  }

  public Configuration(
      TransportStrategy transportStrategy, RetryStrategy retryStrategy, int concurrencyLimit) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = retryStrategy;
    this.concurrencyLimit = concurrencyLimit;
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
  public Configuration withTransportStrategy(final TransportStrategy transportStrategy) {
    return new Configuration(transportStrategy, this.retryStrategy);
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

  public Configuration withTimeout(final Duration timeout) {
    final GrpcConfiguration newGrpcConfiguration =
        this.getTransportStrategy().getGrpcConfiguration().withDeadline(timeout);
    final TransportStrategy newTransportStrategy =
        this.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    return new Configuration(newTransportStrategy, this.retryStrategy);
  }

  public Configuration withRetryStrategy(final RetryStrategy retryStrategy) {
    return new Configuration(this.transportStrategy, retryStrategy);
  }

  public Optional<Integer> getConcurrencyLimit() {
    return Optional.ofNullable(concurrencyLimit);
  }
}
