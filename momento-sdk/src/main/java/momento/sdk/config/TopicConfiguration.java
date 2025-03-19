package momento.sdk.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import momento.sdk.config.middleware.Middleware;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.FixedDelaySubscriptionRetryStrategy;
import momento.sdk.retry.SubscriptionRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The contract for SDK configurables. A configuration must have a transport strategy. */
public class TopicConfiguration {

  private final TransportStrategy transportStrategy;
  private final SubscriptionRetryStrategy subscriptionRetryStrategy;
  private final List<Middleware> middlewares;
  private final Logger logger;

  /**
   * Creates a new topic configuration.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param subscriptionRetryStrategy Responsible for determining when to reconnect a broken
   *     subscription.
   * @param middlewares List of middleware that can intercept and modify calls to Momento.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(
      TransportStrategy transportStrategy,
      SubscriptionRetryStrategy subscriptionRetryStrategy,
      List<Middleware> middlewares,
      Logger logger) {
    this.transportStrategy = transportStrategy;
    this.subscriptionRetryStrategy = subscriptionRetryStrategy;
    this.middlewares = middlewares;
    this.logger = logger;
  }

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(
      TransportStrategy transportStrategy, List<Middleware> middlewares, Logger logger) {
    this(
        transportStrategy,
        new FixedDelaySubscriptionRetryStrategy(Duration.ofMillis(500)),
        middlewares,
        logger);
  }

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param middlewares List of middleware that can intercept and modify calls to Momento.
   */
  public TopicConfiguration(TransportStrategy transportStrategy, List<Middleware> middlewares) {
    this(
        transportStrategy,
        new FixedDelaySubscriptionRetryStrategy(Duration.ofMillis(500)),
        middlewares,
        LoggerFactory.getLogger(TopicConfiguration.class));
  }

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   * @param logger Responsible for logging
   */
  public TopicConfiguration(TransportStrategy transportStrategy, Logger logger) {
    this(transportStrategy, new ArrayList<>(), logger);
  }

  /**
   * Creates a new topic configuration object.
   *
   * @param transportStrategy Responsible for configuring network tunables.
   */
  public TopicConfiguration(TransportStrategy transportStrategy) {
    this(transportStrategy, new ArrayList<>());
  }

  /**
   * Configuration for subscription retries. By default, {@link
   * momento.sdk.retry.FixedDelaySubscriptionRetryStrategy} gets used.
   *
   * @return The subscription retry strategy
   */
  public SubscriptionRetryStrategy getSubscriptionRetryStrategy() {
    return subscriptionRetryStrategy;
  }

  /**
   * Copy constructor that modifies the subscription retry strategy.
   *
   * @param subscriptionRetryStrategy The new subscription retry strategy
   * @return a new TopicConfiguration with the updated subscription retry strategy
   */
  public TopicConfiguration withSubscriptionRetryStrategy(
      @Nonnull final SubscriptionRetryStrategy subscriptionRetryStrategy) {
    return new TopicConfiguration(
        this.transportStrategy, subscriptionRetryStrategy, this.middlewares, this.logger);
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
