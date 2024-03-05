package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.DefaultRetryEligibilityStrategy;
import momento.sdk.retry.FixedCountRetryStrategy;
import momento.sdk.retry.RetryStrategy;

/** Prebuilt {@link Configuration}s for different environments. */
public class Configurations {

  public static final int DEFAULT_MAX_RETRIES = 3;

  /**
   * Provides defaults suitable for a medium-to-high-latency dev environment. Permissive timeouts,
   * retries, and relaxed latency and throughput targets.
   */
  public static class Laptop extends Configuration {

    private Laptop(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
      super(transportStrategy, retryStrategy);
    }

    /**
     * Provides the latest recommended configuration for a dev environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest Laptop configuration
     */
    public static Configuration latest() {
      return Laptop.v1();
    }

    /**
     * Provides v1 configuration for a laptop environment. This configuration is guaranteed not to
     * change in future releases of the Momento Java SDK.
     *
     * @return the v1 laptop configuration
     */
    public static Configuration v1() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(15000)));
      final RetryStrategy retryStrategy =
          new FixedCountRetryStrategy(DEFAULT_MAX_RETRIES, new DefaultRetryEligibilityStrategy());
      return new Laptop(transportStrategy, retryStrategy);
    }
  }

  /**
   * Provides defaults suitable for an environment where your client is running in the same region
   * as the Momento service. It has more aggressive timeouts and retry behavior than the Laptop
   * config.
   */
  public static class InRegion extends Configuration {

    private InRegion(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
      super(transportStrategy, retryStrategy);
    }

    /**
     * Provides the latest recommended configuration for an in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest in-region configuration
     */
    public static Configuration latest() {
      return InRegion.v1();
    }

    /**
     * Provides v1 configuration for an in-region environment. This configuration is guaranteed not
     * to change in future releases of the Momento Java SDK.
     *
     * @return the v1 in-region configuration
     */
    public static Configuration v1() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(1100)));
      final RetryStrategy retryStrategy =
          new FixedCountRetryStrategy(DEFAULT_MAX_RETRIES, new DefaultRetryEligibilityStrategy());
      return new InRegion(transportStrategy, retryStrategy);
    }
  }

  /**
   * This config prioritizes keeping p99.9 latencies as low as possible, potentially sacrificing
   * some throughput to achieve this. Use this configuration if low latency is more important in
   * your application than cache availability.
   */
  public static class LowLatency extends Configuration {

    private LowLatency(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
      super(transportStrategy, retryStrategy);
    }

    /**
     * Provides the latest recommended configuration for a low-latency in-region environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * @return the latest low-latency configuration
     */
    public static Configuration latest() {
      return LowLatency.v1();
    }

    /**
     * Provides v1 configuration for a low-latency environment. This configuration is guaranteed not
     * to change in future releases of the Momento Java SDK.
     *
     * @return the v1 low-latency configuration
     */
    public static Configuration v1() {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(500)));
      final RetryStrategy retryStrategy =
          new FixedCountRetryStrategy(DEFAULT_MAX_RETRIES, new DefaultRetryEligibilityStrategy());
      return new LowLatency(transportStrategy, retryStrategy);
    }
  }

  public static class Lambda extends Configuration {
    private Lambda(TransportStrategy transportStrategy, RetryStrategy retryStrategy) {
      super(transportStrategy, retryStrategy);
    }

    /**
     * Provides the latest recommended configuration for a Lambda environment.
     *
     * <p>This configuration may change in future releases to take advantage of improvements we
     * identify for default configurations.
     *
     * <p>NOTE: keep-alives are very important for long-lived server environments where there may be
     * periods of time when the connection is idle. However, they are very problematic for lambda
     * environments where the lambda runtime is continuously frozen and unfrozen, because the lambda
     * may be frozen before the "ACK" is received from the server. This can cause the keep-alive to
     * timeout even though the connection is completely healthy. Therefore, keep-alives should be
     * disabled in lambda and similar environments.
     *
     * @return the latest Lambda configuration
     */
    public static Configuration latest() {
      final GrpcConfiguration grpcConfig =
          new GrpcConfiguration(Duration.ofMillis(1100))
              .withKeepAliveTime(0)
              .withKeepAliveTimeout(0)
              .withKeepAliveWithoutCalls(false);
      final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);
      final RetryStrategy retryStrategy =
          new FixedCountRetryStrategy(DEFAULT_MAX_RETRIES, new DefaultRetryEligibilityStrategy());
      return new Lambda(transportStrategy, retryStrategy);
    }
  }
}
