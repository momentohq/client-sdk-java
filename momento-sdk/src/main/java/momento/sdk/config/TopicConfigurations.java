package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.config.transport.TransportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Prebuilt {@link TopicConfiguration}s for different environments. */
public class TopicConfigurations {
  /**
   * Provides defaults suitable for a medium-to-high-latency dev environment. Permissive timeouts,
   * relaxed latency and throughput targets.
   */
  public static class Laptop extends TopicConfiguration {

    private Laptop(TransportStrategy transportStrategy, Logger logger) {
      super(transportStrategy, logger);
    }

    /**
     * Provides the latest recommended configuration for a dev environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest Laptop configuration
     */
    public static TopicConfiguration latest() {
      final GrpcConfiguration grpcConfig =
          new GrpcConfiguration(Duration.ofMillis(15000))
              .withKeepAliveTime(10000)
              .withKeepAliveTimeout(5000);
      final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);
      final Logger logger = LoggerFactory.getLogger(TopicConfiguration.class);
      return new Laptop(transportStrategy, logger);
    }
  }

  /**
   * Provides defaults suitable for an environment where your client is running in the same region
   * as the Momento service. It has more aggressive timeout behavior than the Laptop config.
   */
  public static class InRegion extends TopicConfiguration {

    private InRegion(TransportStrategy transportStrategy, Logger logger) {
      super(transportStrategy, logger);
    }

    /**
     * Provides the latest recommended configuration for an in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest in-region configuration
     */
    public static TopicConfiguration latest() {
      final GrpcConfiguration grpcConfig =
          new GrpcConfiguration(Duration.ofMillis(1100))
              .withKeepAliveTime(10000)
              .withKeepAliveTimeout(5000);
      final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);
      final Logger logger = LoggerFactory.getLogger(TopicConfiguration.class);
      return new InRegion(transportStrategy, logger);
    }
  }

  /**
   * This config prioritizes keeping p99.9 latencies as low as possible, potentially sacrificing
   * some throughput to achieve this. Use this configuration if low latency is more important in
   * your application than cache availability.
   */
  public static class LowLatency extends TopicConfiguration {

    private LowLatency(TransportStrategy transportStrategy, Logger logger) {
      super(transportStrategy, logger);
    }

    /**
     * Provides the latest recommended configuration for a low-latency in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest low-latency configuration
     */
    public static TopicConfiguration latest() {
      final GrpcConfiguration grpcConfig =
          new GrpcConfiguration(Duration.ofMillis(500))
              .withKeepAliveTime(10000)
              .withKeepAliveTimeout(5000);
      final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);
      final Logger logger = LoggerFactory.getLogger(TopicConfiguration.class);
      return new LowLatency(transportStrategy, logger);
    }
  }
}
