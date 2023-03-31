package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.config.transport.TransportStrategy;

/** Prebuilt {@link Configuration}s for different environments. */
public class Configurations {

  /**
   * Provides defaults suitable for a medium-to-high-latency dev environment. Permissive timeouts,
   * retries, and relaxed latency and throughput targets.
   */
  public static class Laptop extends Configuration {

    private Laptop(TransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for a dev environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest Laptop configuration
     */
    public static Configuration Latest() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(15000)));
      return new Laptop(transportStrategy);
    }
  }

  /**
   * Provides defaults suitable for an environment where your client is running in the same region
   * as the Momento service. It has more aggressive timeouts and retry behavior than the Laptop
   * config.
   */
  public static class InRegion extends Configuration {

    private InRegion(TransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for an in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest in-region configuration
     */
    public static Configuration Latest() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(1100)));
      return new Laptop(transportStrategy);
    }
  }

  /**
   * This config prioritizes keeping p99.9 latencies as low as possible, potentially sacrificing
   * some throughput to achieve this. Use this configuration if low latency is more important in
   * your application than cache availability.
   */
  public static class LowLatency extends Configuration {

    private LowLatency(TransportStrategy transportStrategy) {
      super(transportStrategy);
    }

    /**
     * Provides the latest recommended configuration for a low-latency in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest low-latency configuration
     */
    public static Configuration Latest() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(500)));
      return new Laptop(transportStrategy);
    }
  }
}
