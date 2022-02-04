package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests client side exceptions */
final class SimpleCacheDataPlaneClientSideTest extends BaseTestClass {

  private static final int DEFAULT_ITEM_TTL_SECONDS = 60;
  private String authToken;
  private String cacheName;
  private SimpleCacheClient client;

  @BeforeEach
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    cacheName = System.getenv("TEST_CACHE_NAME");
    client = SimpleCacheClient.builder(authToken, DEFAULT_ITEM_TTL_SECONDS).build();
  }

  @AfterEach
  void teardown() {
    client.close();
  }

  @Test
  public void nullKeyGetThrowsException() {
    String nullKeyString = null;
    assertThrows(InvalidArgumentException.class, () -> client.get(cacheName, nullKeyString));
    assertThrows(InvalidArgumentException.class, () -> client.getAsync(cacheName, nullKeyString));

    byte[] nullByteKey = null;
    assertThrows(InvalidArgumentException.class, () -> client.get(cacheName, nullByteKey));
    assertThrows(InvalidArgumentException.class, () -> client.getAsync(cacheName, nullByteKey));
  }

  @Test
  public void nullKeySetThrowsException() {
    String nullKeyString = null;
    // Blocking String key set
    assertThrows(
        InvalidArgumentException.class, () -> client.set(cacheName, nullKeyString, "hello", 10));
    assertThrows(
        ClientSdkException.class,
        () -> client.set(cacheName, nullKeyString, ByteBuffer.allocate(1), 10));
    // Async String key set
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, nullKeyString, "hello", 10));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, nullKeyString, ByteBuffer.allocate(1), 10));

    byte[] nullByteKey = null;
    assertThrows(
        InvalidArgumentException.class,
        () -> client.set(cacheName, nullByteKey, new byte[] {0x00}, 10));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, nullByteKey, new byte[] {0x00}, 10));
  }

  @Test
  public void nullValueSetThrowsException() {
    assertThrows(
        InvalidArgumentException.class, () -> client.set(cacheName, "hello", (String) null, 10));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.set(cacheName, "hello", (ByteBuffer) null, 10));
    assertThrows(
        InvalidArgumentException.class, () -> client.set(cacheName, new byte[] {}, null, 10));

    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, "hello", (String) null, 10));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, "hello", (ByteBuffer) null, 10));
    assertThrows(
        InvalidArgumentException.class, () -> client.setAsync(cacheName, new byte[] {}, null, 10));
  }

  @Test
  public void ttlMustNotBeNegativeThrowsException() {
    assertThrows(InvalidArgumentException.class, () -> client.set(cacheName, "hello", "world", -1));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.set(cacheName, "hello", ByteBuffer.allocate(1), -1));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.set(cacheName, new byte[] {}, new byte[] {}, -1));

    assertThrows(InvalidArgumentException.class, () -> client.setAsync(cacheName, "hello", "", -1));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, "hello", ByteBuffer.allocate(1), -1));
    assertThrows(
        InvalidArgumentException.class,
        () -> client.setAsync(cacheName, new byte[] {}, new byte[] {}, -1));
  }

  @Test
  public void nullCacheNameThrowsException() {
    assertThrows(InvalidArgumentException.class, () -> client.get(null, ""));
    assertThrows(InvalidArgumentException.class, () -> client.set(null, "", "", 10));

    assertThrows(InvalidArgumentException.class, () -> client.getAsync(null, "").get());
    assertThrows(InvalidArgumentException.class, () -> client.setAsync(null, "", "", 10).get());
  }
}
