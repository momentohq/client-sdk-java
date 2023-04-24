package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests client side exceptions */
final class CacheDataPlaneClientSideTest extends BaseTestClass {

  private static final Duration DEFAULT_ITEM_TTL_SECONDS = Duration.ofSeconds(60);

  private final String cacheName = System.getenv("TEST_CACHE_NAME");
  private CacheClient client;

  @BeforeEach
  void setup() {
    client =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL_SECONDS)
            .build();
  }

  @AfterEach
  void teardown() {
    client.close();
  }

  @Test
  public void nullKeyGetReturnsError() {
    final GetResponse stringResponse = client.get(cacheName, (String) null).join();
    assertThat(stringResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) stringResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final GetResponse byteResponse = client.get(cacheName, (byte[]) null).join();
    assertThat(byteResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) byteResponse).hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullKeyDeleteReturnsError() {
    final DeleteResponse stringKeyResponse = client.delete(cacheName, (String) null).join();
    assertThat(stringKeyResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) stringKeyResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final DeleteResponse byteKeyResponse = client.delete(cacheName, (byte[]) null).join();
    assertThat(byteKeyResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) byteKeyResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullKeySetReturnsError() {
    final SetResponse stringSetResponse =
        client.set(cacheName, null, "hello", Duration.ofSeconds(10)).join();
    assertThat(stringSetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringSetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteKeySetResponse =
        client.set(cacheName, null, new byte[] {0x00}, Duration.ofSeconds(10)).join();
    assertThat(byteKeySetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteKeySetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullValueSetReturnsError() {
    final SetResponse stringResponse =
        client.set(cacheName, "hello", null, Duration.ofSeconds(10)).join();
    assertThat(stringResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteArrayResponse =
        client.set(cacheName, new byte[] {}, null, Duration.ofSeconds(10)).join();
    assertThat(byteArrayResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteArrayResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void ttlMustNotBeNegativeReturnsError() {
    final SetResponse stringSetResponse =
        client.set(cacheName, "hello", "", Duration.ofSeconds(-1)).join();
    assertThat(stringSetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringSetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteArraySetResponse =
        client.set(cacheName, new byte[] {}, new byte[] {}, Duration.ofSeconds(-1)).join();
    assertThat(byteArraySetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteArraySetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullCacheNameReturnsError() {
    final GetResponse getResponse = client.get(null, "").join();
    assertThat(getResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) getResponse).hasCauseInstanceOf(InvalidArgumentException.class);

    final DeleteResponse deleteResponse = client.delete(null, "").join();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) deleteResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse setResponse = client.set(null, "", "", Duration.ofSeconds(10)).join();
    assertThat(setResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) setResponse).hasCauseInstanceOf(InvalidArgumentException.class);
  }
}
