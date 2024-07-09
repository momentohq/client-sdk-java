package momento.sdk;

import static momento.sdk.BaseTestClass.credentialProvider;
import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.config.Configurations;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import org.junit.jupiter.api.Test;

public class CacheDataPlaneEagerConnectionTest extends BaseTestClass {
  @Test
  void getReturnsHitAfterSet() {
    CacheClient client =
        CacheClient.create(
            credentialProvider,
            Configurations.Laptop.latest(),
            DEFAULT_TTL_SECONDS,
            Duration.ofSeconds(10));
    final String key = randomString("key");
    final String value = randomString("value");

    // Successful Set
    final SetResponse setResponse = client.set(cacheName, key, value).join();
    assertThat(setResponse).isInstanceOf(SetResponse.Success.class);
    assertThat(((SetResponse.Success) setResponse).valueString()).isEqualTo(value);
    assertThat(((SetResponse.Success) setResponse).valueByteArray()).isEqualTo(value.getBytes());

    // Successful Get with Hit
    final GetResponse getResponse = client.get(cacheName, key).join();
    assertThat(getResponse).isInstanceOf(GetResponse.Hit.class);
    assertThat(((GetResponse.Hit) getResponse).valueString()).isEqualTo(value);
  }
}
