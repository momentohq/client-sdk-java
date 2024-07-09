package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.cache.DeleteResponse;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.SetResponse;
import org.junit.jupiter.api.Test;

/** Tests client side exceptions */
final class CacheDataPlaneClientSideTest extends BaseCacheTestClass {
  @Test
  public void nullKeyGetReturnsError() {
    final GetResponse stringResponse = cacheClient.get(cacheName, (String) null).join();
    assertThat(stringResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) stringResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final GetResponse byteResponse = cacheClient.get(cacheName, (byte[]) null).join();
    assertThat(byteResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) byteResponse).hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullKeyDeleteReturnsError() {
    final DeleteResponse stringKeyResponse = cacheClient.delete(cacheName, (String) null).join();
    assertThat(stringKeyResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) stringKeyResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final DeleteResponse byteKeyResponse = cacheClient.delete(cacheName, (byte[]) null).join();
    assertThat(byteKeyResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) byteKeyResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullKeySetReturnsError() {
    final SetResponse stringSetResponse =
        cacheClient.set(cacheName, null, "hello", Duration.ofSeconds(10)).join();
    assertThat(stringSetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringSetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteKeySetResponse =
        cacheClient.set(cacheName, null, new byte[] {0x00}, Duration.ofSeconds(10)).join();
    assertThat(byteKeySetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteKeySetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullValueSetReturnsError() {
    final SetResponse stringResponse =
        cacheClient.set(cacheName, "hello", null, Duration.ofSeconds(10)).join();
    assertThat(stringResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteArrayResponse =
        cacheClient.set(cacheName, new byte[] {}, null, Duration.ofSeconds(10)).join();
    assertThat(byteArrayResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteArrayResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void ttlMustNotBeNegativeReturnsError() {
    final SetResponse stringSetResponse =
        cacheClient.set(cacheName, "hello", "", Duration.ofSeconds(-1)).join();
    assertThat(stringSetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) stringSetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse byteArraySetResponse =
        cacheClient.set(cacheName, new byte[] {}, new byte[] {}, Duration.ofSeconds(-1)).join();
    assertThat(byteArraySetResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) byteArraySetResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void nullCacheNameReturnsError() {
    final GetResponse getResponse = cacheClient.get(null, "").join();
    assertThat(getResponse).isInstanceOf(GetResponse.Error.class);
    assertThat((GetResponse.Error) getResponse).hasCauseInstanceOf(InvalidArgumentException.class);

    final DeleteResponse deleteResponse = cacheClient.delete(null, "").join();
    assertThat(deleteResponse).isInstanceOf(DeleteResponse.Error.class);
    assertThat((DeleteResponse.Error) deleteResponse)
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final SetResponse setResponse = cacheClient.set(null, "", "", Duration.ofSeconds(10)).join();
    assertThat(setResponse).isInstanceOf(SetResponse.Error.class);
    assertThat((SetResponse.Error) setResponse).hasCauseInstanceOf(InvalidArgumentException.class);
  }
}
