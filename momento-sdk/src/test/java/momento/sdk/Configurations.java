package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import momento.sdk.config.Configuration;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.transport.GrpcConfiguration;
import momento.sdk.config.transport.StaticTransportStrategy;
import momento.sdk.config.transport.TransportStrategy;
import org.junit.jupiter.api.Test;

public class Configurations {
  @Test
  public void testCacheLambdaConfigurationDisablesKeepalive() {
    final Configuration config = momento.sdk.config.Configurations.Lambda.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertThat(grpcConfig.getKeepAliveWithoutCalls().isEmpty());
    assertTrue(grpcConfig.getKeepAliveTimeMs().isEmpty());
    assertTrue(grpcConfig.getKeepAliveTimeoutMs().isEmpty());
  }

  @Test
  public void testCacheLaptopConfigurationEnablesKeepalive() {
    final Configuration config = momento.sdk.config.Configurations.Laptop.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertTrue(grpcConfig.getKeepAliveWithoutCalls().get());
    assertThat(grpcConfig.getKeepAliveTimeMs().get()).isEqualTo(5000);
    assertThat(grpcConfig.getKeepAliveTimeoutMs().get()).isEqualTo(1000);
  }

  @Test
  public void testTopicsLaptopConfigurationEnablesKeepalive() {
    final TopicConfiguration config = momento.sdk.config.TopicConfigurations.Laptop.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertTrue(grpcConfig.getKeepAliveWithoutCalls().get());
    assertThat(grpcConfig.getKeepAliveTimeMs().get()).isEqualTo(10000);
    assertThat(grpcConfig.getKeepAliveTimeoutMs().get()).isEqualTo(5000);
  }

  @Test
  public void testTopicsLaptopConfigurationNumChannels() {
    final TopicConfiguration config = momento.sdk.config.TopicConfigurations.Laptop.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertEquals(4, grpcConfig.getNumUnaryGrpcChannels()); // Default value
    assertEquals(4, grpcConfig.getNumStreamGrpcChannels()); // Default value
    assertEquals(4, grpcConfig.getMinNumGrpcChannels()); // Default value
  }

  @Test
  public void testTopicsConfigurationWithSeparateUnaryAndStreamChannels_WhenSetExplicitly() {
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000))
            .withNumUnaryGrpcChannels(2)
            .withNumStreamGrpcChannels(3);
    final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);

    assertEquals(2, transportStrategy.getGrpcConfiguration().getNumUnaryGrpcChannels());
    assertEquals(3, transportStrategy.getGrpcConfiguration().getNumStreamGrpcChannels());
    assertEquals(4, transportStrategy.getGrpcConfiguration().getMinNumGrpcChannels()); // Default value
    assertEquals(Duration.ofMillis(15000), transportStrategy.getGrpcConfiguration().getDeadline());
  }

  @Test
  public void testTopicsConfigurationWithSeparateUnaryAndStreamChannels_WhenNotSetExplicitly() {
    final GrpcConfiguration grpcConfig =
        new GrpcConfiguration(Duration.ofMillis(15000)).withMinNumGrpcChannels(2);
    final TransportStrategy transportStrategy = new StaticTransportStrategy(grpcConfig);

    assertEquals(2, transportStrategy.getGrpcConfiguration().getNumUnaryGrpcChannels()); // Fallback to minNumGrpcChannels
    assertEquals(2, transportStrategy.getGrpcConfiguration().getNumStreamGrpcChannels()); // Fallback to minNumGrpcChannels
    assertEquals(2, transportStrategy.getGrpcConfiguration().getMinNumGrpcChannels());
    assertEquals(Duration.ofMillis(15000), transportStrategy.getGrpcConfiguration().getDeadline());
  }
}
