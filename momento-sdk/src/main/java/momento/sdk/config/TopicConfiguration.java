package momento.sdk.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.transport.topics.TopicsGrpcConfiguration;
import momento.sdk.config.transport.topics.TopicsTransportStrategy;
import org.slf4j.Logger;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class TopicConfiguration {

  private final TopicsTransportStrategy transportStrategy;
  private final List<Middleware> middlewares;
  private final Logger logger;

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(
      TopicsTransportStrategy transportStrategy, List<Middleware> middlewares, Logger logger) {
    this.transportStrategy = transportStrategy;
    this.middlewares = middlewares;
    this.logger = logger;
  }

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(TopicsTransportStrategy transportStrategy, Logger logger) {
    this(transportStrategy, new ArrayList<>(), logger);
  }

  /**
   * Configuration for network tunables.
   *
   * @return The transport strategy
   */
  public TopicsTransportStrategy getTransportStrategy() {
    return transportStrategy;
  }

  public TopicConfiguration withTimeout(final Duration timeout) {
    final TopicsGrpcConfiguration newGrpcConfiguration =
        this.getTransportStrategy().getGrpcConfiguration().withDeadline(timeout);
    final TopicsTransportStrategy newTransportStrategy =
        this.getTransportStrategy().withGrpcConfiguration(newGrpcConfiguration);
    return new TopicConfiguration(newTransportStrategy, this.logger);
  }

  /**
   * Copy constructor that adds a middleware.
   *
   * @param middleware The new middleware.
   * @return a new TopicConfiguration with the updated middleware.
   */
  public TopicConfiguration withMiddleware(@Nonnull final Middleware middleware) {
    final List<Middleware> newMiddlewares = new ArrayList<>(this.middlewares);
    newMiddlewares.add(middleware);
    return new TopicConfiguration(this.transportStrategy, newMiddlewares, this.logger);
  }

  /**
   * Get the middleware to be applied to the request.
   *
   * @return The middleware
   */
  public List<Middleware> getMiddlewares() {
    return middlewares;
  }
}
