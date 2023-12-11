package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;
import org.slf4j.Logger;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class TopicConfiguration {

  private final TransportStrategy transportStrategy;
  private final Logger logger;

  /**
   * Creates a new configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(TransportStrategy transportStrategy, Logger logger) {
    this.transportStrategy = transportStrategy;
    this.logger = logger;
  }

  /**
   * Configuration for network tunables.
   *
   * @return The transport strategy
   */
  public TransportStrategy getTransportStrategy() {
    return transportStrategy;
  }

  public TopicConfiguration withTimeout(final Duration timeout) {
    final GrpcConfiguration newGrpcConfiguration =
        this.getTransportStrategy().getGrpcConfiguration().withDeadline(timeout);
    final TransportStrategy newTransportStrategy =
        this.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    return new TopicConfiguration(newTransportStrategy, this.logger);
  }
}
