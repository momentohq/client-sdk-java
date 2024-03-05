package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import momento.sdk.config.Configuration;
import momento.sdk.config.TopicConfiguration;
import momento.sdk.config.transport.GrpcConfiguration;
import org.junit.jupiter.api.Test;

public class Configurations {
  @Test
  public void testCacheLambdaConfigurationDisablesKeepalive() {
    final Configuration config = momento.sdk.config.Configurations.Lambda.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertFalse(grpcConfig.getKeepAliveWithoutCalls());
    assertThat(grpcConfig.getKeepAliveTimeMs()).isEqualTo(0);
    assertThat(grpcConfig.getKeepAliveTimeoutMs()).isEqualTo(0);
  }

  @Test
  public void testCacheLaptopConfigurationEnablesKeepalive() {
    final Configuration config = momento.sdk.config.Configurations.Laptop.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertTrue(grpcConfig.getKeepAliveWithoutCalls());
    assertThat(grpcConfig.getKeepAliveTimeMs()).isEqualTo(5000);
    assertThat(grpcConfig.getKeepAliveTimeoutMs()).isEqualTo(1000);
  }

  @Test
  public void testTopicsLaptopConfigurationEnablesKeepalive() {
    final TopicConfiguration config = momento.sdk.config.TopicConfigurations.Laptop.latest();
    final GrpcConfiguration grpcConfig = config.getTransportStrategy().getGrpcConfiguration();
    assertTrue(grpcConfig.getKeepAliveWithoutCalls());
    assertThat(grpcConfig.getKeepAliveTimeMs()).isEqualTo(10000);
    assertThat(grpcConfig.getKeepAliveTimeoutMs()).isEqualTo(5000);
  }
}
