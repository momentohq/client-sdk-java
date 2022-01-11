package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheSetResponse;
import momento.sdk.messages.MomentoCacheResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheTest {

  private static final int DEFAULT_TTL_SECONDS = 60;

  private Momento momento;
  private Cache cache;

  @BeforeAll
  static void beforeAll() {
    if (System.getenv("TEST_AUTH_TOKEN") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_AUTH_TOKEN env var; see README for more details.");
    }
    if (System.getenv("TEST_CACHE_NAME") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_CACHE_NAME env var; see README for more details.");
    }
  }

  @BeforeEach
  void setup() {
    momento = Momento.builder(System.getenv("TEST_AUTH_TOKEN")).build();
    cache = momento.cacheBuilder(System.getenv("TEST_CACHE_NAME"), DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void tearDown() {
    momento.close();
  }

  @Test
  public void blockingCacheGetSetSuccess() {
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    cache.set(key, value);
    String actual = cache.get(key).string().get();

    assertEquals(value, actual);
  }

  @Test
  public void asyncCacheGetSetSuccess() throws Exception {
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    CompletableFuture<CacheSetResponse> response = cache.setAsync(key, value);
    assertEquals(MomentoCacheResult.Ok, response.get().result());

    CompletableFuture<CacheGetResponse> getResponse = cache.getAsync(key);
    assertEquals(value, getResponse.get().string().get());
    assertEquals(MomentoCacheResult.Hit, getResponse.get().result());
  }

  @Test
  public void blockingCacheMissHandlingSuccess() {
    CacheGetResponse response = cache.get(UUID.randomUUID().toString());

    assertEquals(MomentoCacheResult.Miss, response.result());
  }

  @Test
  public void asyncCacheMissSuccess() throws Exception {
    CompletableFuture<CacheGetResponse> getResponse = cache.getAsync(UUID.randomUUID().toString());
    assertEquals(MomentoCacheResult.Miss, getResponse.get().result());
  }

  @Test
  public void throwsCacheNotFoundForNonexistentCacheOnGetSetBlocking() {
    Cache cache = momento.cacheBuilder(UUID.randomUUID().toString(), DEFAULT_TTL_SECONDS).build();

    assertThrows(CacheNotFoundException.class, () -> cache.get("testKey"));
    assertThrows(CacheNotFoundException.class, () -> cache.set("testKey", "value"));
  }

  @Test
  public void throwsCacheNotFoundForNonexistentCacheOnGetSetAsync() {
    Cache cache = momento.cacheBuilder(UUID.randomUUID().toString(), DEFAULT_TTL_SECONDS).build();

    ExecutionException getException =
        assertThrows(ExecutionException.class, () -> cache.getAsync("testKey").get());
    assertTrue(getException.getCause() instanceof CacheNotFoundException);

    ExecutionException setException =
        assertThrows(ExecutionException.class, () -> cache.setAsync("testKey", "value").get());
    assertTrue(setException.getCause() instanceof CacheNotFoundException);
  }

  @Test
  public void setAndGetWithByteKeyValuesMustSucceed() {
    byte[] key = {0x01, 0x02, 0x03, 0x04};
    byte[] value = {0x05, 0x06, 0x07, 0x08};

    CacheSetResponse setResponse = cache.set(key, value, 60);
    assertEquals(MomentoCacheResult.Ok, setResponse.result());

    CacheGetResponse getResponse = cache.get(key);
    assertEquals(MomentoCacheResult.Hit, getResponse.result());
    assertArrayEquals(value, getResponse.byteArray().get());
  }

  @Test
  public void nullKeyGetThrowsException() {
    String nullKeyString = null;
    assertThrows(ClientSdkException.class, () -> cache.get(nullKeyString));
    assertThrows(ClientSdkException.class, () -> cache.getAsync(nullKeyString));

    byte[] nullByteKey = null;
    assertThrows(ClientSdkException.class, () -> cache.get(nullByteKey));
    assertThrows(ClientSdkException.class, () -> cache.getAsync(nullByteKey));
  }

  @Test
  public void nullKeySetThrowsException() {
    String nullKeyString = null;
    // Blocking String key set
    assertThrows(ClientSdkException.class, () -> cache.set(nullKeyString, "hello", 10));
    assertThrows(
        ClientSdkException.class, () -> cache.set(nullKeyString, ByteBuffer.allocate(1), 10));
    // Async String key set
    assertThrows(ClientSdkException.class, () -> cache.setAsync(nullKeyString, "hello", 10));
    assertThrows(
        ClientSdkException.class, () -> cache.setAsync(nullKeyString, ByteBuffer.allocate(1), 10));

    byte[] nullByteKey = null;
    assertThrows(ClientSdkException.class, () -> cache.set(nullByteKey, new byte[] {0x00}, 10));
    assertThrows(
        ClientSdkException.class, () -> cache.setAsync(nullByteKey, new byte[] {0x00}, 10));
  }

  @Test
  public void nullValueSetThrowsException() {
    assertThrows(ClientSdkException.class, () -> cache.set("hello", (String) null, 10));
    assertThrows(ClientSdkException.class, () -> cache.set("hello", (ByteBuffer) null, 10));
    assertThrows(ClientSdkException.class, () -> cache.set(new byte[] {}, null, 10));

    assertThrows(ClientSdkException.class, () -> cache.setAsync("hello", (String) null, 10));
    assertThrows(ClientSdkException.class, () -> cache.setAsync("hello", (ByteBuffer) null, 10));
    assertThrows(ClientSdkException.class, () -> cache.setAsync(new byte[] {}, null, 10));
  }

  @Test
  public void ttlMustBePositiveThrowsException() {
    for (int i = -1; i <= 0; i++) {
      final int j = i;
      assertThrows(ClientSdkException.class, () -> cache.set("hello", "world", j));
      assertThrows(ClientSdkException.class, () -> cache.set("hello", ByteBuffer.allocate(1), j));
      assertThrows(ClientSdkException.class, () -> cache.set(new byte[] {}, new byte[] {}, j));
    }

    for (int i = -1; i <= 0; i++) {
      final int j = i;

      assertThrows(ClientSdkException.class, () -> cache.setAsync("hello", "", j));
      assertThrows(
          ClientSdkException.class, () -> cache.setAsync("hello", ByteBuffer.allocate(1), j));
      assertThrows(ClientSdkException.class, () -> cache.setAsync(new byte[] {}, new byte[] {}, j));
    }
  }
}
