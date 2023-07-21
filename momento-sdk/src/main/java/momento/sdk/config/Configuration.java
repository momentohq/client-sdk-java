package momento.sdk.config;

import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.DefaultRetryEligibilityStrategy;
import momento.sdk.retry.FixedCountRetryStrategy;
import momento.sdk.retry.RetryEligibilityStrategy;
import momento.sdk.retry.RetryStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class Configuration {

  private final TransportStrategy transportStrategy;
  private final RetryStrategy retryStrategy;
  private final RetryEligibilityStrategy retryEligibilityStrategy;

  public static final int MAX_RETRIES = 3;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   * @param retryEligibilityStrategy Responsible for configuring retry eligbility
   */
  public Configuration(
      TransportStrategy transportStrategy,
      RetryStrategy retryStrategy,
      RetryEligibilityStrategy retryEligibilityStrategy) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = retryStrategy;
    this.retryEligibilityStrategy = retryEligibilityStrategy;
  }

  /**
   * Creates a new configuration object with default retry and eligibility strategies
   *
   * @param transportStrategy
   */
  public Configuration(TransportStrategy transportStrategy) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = new FixedCountRetryStrategy(MAX_RETRIES);
    this.retryEligibilityStrategy = new DefaultRetryEligibilityStrategy();
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
   * Configuration for retry tunables. By default, {@link momento.sdk.retry.FixedDelayRetryStrategy}
   * gets used.
   *
   * @return The retry strategy
   */
  public RetryStrategy getRetryStrategy() {
    return retryStrategy;
  }

  /**
   * Strategy to determine if a request is eligible for retry. By default, {@link
   * DefaultRetryEligibilityStrategy} gets used.
   *
   * @return The retry eligibility strategy
   */
  public RetryEligibilityStrategy getRetryEligibilityStrategy() {
    return retryEligibilityStrategy;
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

  /**
   * Creates a new instance of the configuration object updated to use the given strategies.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   * @param retryEligibilityStrategy Responsible for configuring retry eligibility
   * @return A copy of this Configuration using the new strategies
   */
  public Configuration withTransportStrategy(
      TransportStrategy transportStrategy,
      RetryStrategy retryStrategy,
      RetryEligibilityStrategy retryEligibilityStrategy) {
    return new Configuration(transportStrategy, retryStrategy, retryEligibilityStrategy);
  }
}
