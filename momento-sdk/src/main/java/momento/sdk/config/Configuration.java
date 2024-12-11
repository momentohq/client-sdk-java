package momento.sdk.config;

import java.time.Duration;
import javax.annotation.Nonnull;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.RetryStrategy;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class Configuration {

  private final TransportStrategy transportStrategy;
  private final RetryStrategy retryStrategy;
  private final ReadConcern readConcern;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   * @param readConcern The client-wide setting for read-after-write consistency.
   */
  public Configuration(
      @Nonnull TransportStrategy transportStrategy,
      @Nonnull RetryStrategy retryStrategy,
      @Nonnull ReadConcern readConcern) {
    this.transportStrategy = transportStrategy;
    this.retryStrategy = retryStrategy;
    this.readConcern = readConcern;
  }

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param retryStrategy Responsible for configuring retries
   */
  public Configuration(
      @Nonnull TransportStrategy transportStrategy, @Nonnull RetryStrategy retryStrategy) {
    this(transportStrategy, retryStrategy, ReadConcern.BALANCED);
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
   * @param transportStrategy The new transport strategy
   * @return a new Configuration with the updated transport strategy
   */
  public Configuration withTransportStrategy(@Nonnull final TransportStrategy transportStrategy) {
    return new Configuration(transportStrategy, this.retryStrategy, this.readConcern);
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
   * @param retryStrategy The new retry strategy
   * @return a new Configuration with the updated retry strategy
   */
  public Configuration withRetryStrategy(@Nonnull final RetryStrategy retryStrategy) {
    return new Configuration(this.transportStrategy, retryStrategy, this.readConcern);
  }

  /**
   * The read consistency setting.
   *
   * @return The read concern
   */
  public ReadConcern getReadConcern() {
    return readConcern;
  }

  /**
   * Copy constructor that modifies the read concern.
   *
   * @param readConcern The new read concern setting
   * @return a new Configuration with the updated read concern
   */
  public Configuration withReadConcern(@Nonnull final ReadConcern readConcern) {
    return new Configuration(this.transportStrategy, this.retryStrategy, readConcern);
  }

  /**
   * Copy constructor that modifies the request timeout.
   *
   * @param timeout The new request timeout.
   * @return a new Configuration with the updated timeout.
   */
  public Configuration withTimeout(@Nonnull final Duration timeout) {
    final GrpcConfiguration newGrpcConfiguration =
        this.getTransportStrategy().getGrpcConfiguration().withDeadline(timeout);
    final TransportStrategy newTransportStrategy =
        this.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    return new Configuration(newTransportStrategy, this.retryStrategy, this.readConcern);
  }
}
