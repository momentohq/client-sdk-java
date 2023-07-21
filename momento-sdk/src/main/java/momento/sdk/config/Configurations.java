package momento.sdk.config;

import java.time.Duration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.config.transport.TransportStrategy;
import momento.sdk.retry.DefaultRetryEligibilityStrategy;
import momento.sdk.retry.FixedCountRetryStrategy;
import momento.sdk.retry.RetryEligibilityStrategy;
import momento.sdk.retry.RetryStrategy;

/** Prebuilt {@link Configuration}s for different environments. */
public class Configurations {

  /**
   * Provides defaults suitable for a medium-to-high-latency dev environment. Permissive timeouts,
   * retries, and relaxed latency and throughput targets.
   */
  public static class Laptop extends Configuration {

    private Laptop(
        TransportStrategy transportStrategy,
        RetryStrategy retryStrategy,
        RetryEligibilityStrategy retryEligibilityStrategy) {
      super(transportStrategy, retryStrategy, retryEligibilityStrategy);
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
      return new Laptop(
          transportStrategy,
          new FixedCountRetryStrategy(Configuration.MAX_RETRIES),
          new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for a laptop environment with a custom {@link RetryStrategy}. This
     * configuration is guaranteed not to change in future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @return the v1 laptop configuration
     */
    public static Configuration v1(RetryStrategy retryStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(15000)));
      return new Laptop(transportStrategy, retryStrategy, new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for a laptop environment with a custom {@link RetryStrategy} and a
     * {@link RetryEligibilityStrategy}
     *
     * <p>This configuration is guaranteed not to change in future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @param retryEligibilityStrategy the retry eligibility strategy
     * @return the v1 laptop configuration
     */
    public static Configuration v1(
        RetryStrategy retryStrategy, RetryEligibilityStrategy retryEligibilityStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(15000)));
      return new Laptop(transportStrategy, retryStrategy, retryEligibilityStrategy);
    }
  }

  /**
   * Provides defaults suitable for an environment where your client is running in the same region
   * as the Momento service. It has more aggressive timeouts and retry behavior than the Laptop
   * config.
   */
  public static class InRegion extends Configuration {

    private InRegion(
        TransportStrategy transportStrategy,
        RetryStrategy retryStrategy,
        RetryEligibilityStrategy retryEligibilityStrategy) {
      super(transportStrategy, retryStrategy, retryEligibilityStrategy);
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
      return new InRegion(
          transportStrategy,
          new FixedCountRetryStrategy(Configuration.MAX_RETRIES),
          new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for an in-region environment with a custom {@link RetryStrategy}.
     * This configuration is guaranteed not to change in future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @return the v1 in-region configuration
     */
    public static Configuration v1(RetryStrategy retryStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(1100)));
      return new InRegion(transportStrategy, retryStrategy, new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for an in-region environment with a custom {@link RetryStrategy}
     * and a {@link RetryEligibilityStrategy}
     *
     * <p>This configuration is guaranteed not to change in future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @param retryEligibilityStrategy the retry eligibility strategy
     * @return the v1 in-region configuration
     */
    public static Configuration v1(
        RetryStrategy retryStrategy, RetryEligibilityStrategy retryEligibilityStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(1100)));
      return new InRegion(transportStrategy, retryStrategy, retryEligibilityStrategy);
    }
  }

  /**
   * This config prioritizes keeping p99.9 latencies as low as possible, potentially sacrificing
   * some throughput to achieve this. Use this configuration if low latency is more important in
   * your application than cache availability.
   */
  public static class LowLatency extends Configuration {

    private LowLatency(
        TransportStrategy transportStrategy,
        RetryStrategy retryStrategy,
        RetryEligibilityStrategy retryEligibilityStrategy) {
      super(transportStrategy, retryStrategy, retryEligibilityStrategy);
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
      return new LowLatency(
          transportStrategy,
          new FixedCountRetryStrategy(Configuration.MAX_RETRIES),
          new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for a low-latency environment with a custom {@link RetryStrategy}.
     * This configuration is guaranteed not to change in future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @return the v1 low-latency configuration
     */
    public static Configuration v1(RetryStrategy retryStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(500)));
      return new LowLatency(
          transportStrategy, retryStrategy, new DefaultRetryEligibilityStrategy());
    }

    /**
     * Provides v1 configuration for a low-latency environment with a custom {@link RetryStrategy}
     * and a {@link RetryEligibilityStrategy} This configuration is guaranteed not to change in
     * future releases of the Momento Java SDK.
     *
     * @param retryStrategy the retry strategy
     * @param retryEligibilityStrategy the retry eligibility strategy
     * @return the v1 low-latency configuration
     */
    public static Configuration v1(
        RetryStrategy retryStrategy, RetryEligibilityStrategy retryEligibilityStrategy) {
      final TransportStrategy transportStrategy =
          new StaticTransportStrategy(new GrpcConfiguration(Duration.ofMillis(500)));
      return new LowLatency(transportStrategy, retryStrategy, retryEligibilityStrategy);
    }
  }
}
