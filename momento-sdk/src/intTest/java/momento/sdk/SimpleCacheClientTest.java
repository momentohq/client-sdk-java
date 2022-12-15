package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.messages.CacheGetResponse;
import momento.sdk.messages.CacheGetStatus;
import momento.sdk.messages.CacheSetResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Just includes a happy test path that interacts with both control and data plane clients. */
final class SimpleCacheClientTest extends BaseTestClass {

  private static final int DEFAULT_TTL_SECONDS = 60;

  private SimpleCacheClient target;

  private static final String JWT_HEADER_BASE64 = "eyJhbGciOiJIUzUxMiJ9";
  private static final String JWT_INVALID_SIGNATURE_BASE64 =
      "gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";

  // {"sub":"squirrel","cp":"invalidcontrol.cell-alpha-dev.preprod.a.momentohq.com","c":"cache.cell-alpha-dev.preprod.a.momentohq.com"}
  private static final String JWT_PAYLOAD_BAD_CONTROL_PLANE_BASE64 =
      "eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiaW52YWxpZGNvbnRyb2wuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vbWVudG9ocS5jb20iLCJjIjoiY2FjaGUuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vbWVudG9ocS5jb20ifQ";
  // {"sub":"squirrel","cp":"control.cell-alpha-dev.preprod.a.momentohq.com","c":"invalidcache.cell-alpha-dev.preprod.a.momentohq.com"}
  private static final String JWT_PAYLOAD_BAD_DATA_PLANE_BASE64 =
      "eyJzdWIiOiJzcXVpcnJlbCIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJpbnZhbGlkY2FjaGUuY2VsbC1hbHBoYS1kZXYucHJlcHJvZC5hLm1vbWVudG9ocS5jb20ifQ";

  // These JWTs will result in UNAUTHENTICATED from the reachable backend since they have made up
  // signatures
  private String BAD_CONTROL_PLANE_JWT =
      JWT_HEADER_BASE64
          + "."
          + JWT_PAYLOAD_BAD_CONTROL_PLANE_BASE64
          + "."
          + JWT_INVALID_SIGNATURE_BASE64;
  private String BAD_DATA_PLANE_JWT =
      JWT_HEADER_BASE64
          + "."
          + JWT_PAYLOAD_BAD_DATA_PLANE_BASE64
          + "."
          + JWT_INVALID_SIGNATURE_BASE64;

  @BeforeEach
  void setup() {
    target =
        SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void teardown() {
    target.close();
  }

  @Test
  public void createCacheGetSetDeleteValuesAndDeleteCache() {
    String cacheName = UUID.randomUUID().toString();
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    target.createCache(cacheName);
    CacheSetResponse response = target.set(cacheName, key, value);

    CacheGetResponse getResponse = target.get(cacheName, key);
    assertEquals(CacheGetStatus.HIT, getResponse.status());
    assertEquals(value, getResponse.string().get());

    target.delete(cacheName, key);
    CacheGetResponse getAfterDeleteResponse = target.get(cacheName, key);
    assertEquals(CacheGetStatus.MISS, getAfterDeleteResponse.status());

    CacheGetResponse getForKeyInSomeOtherCache = target.get(System.getenv("TEST_CACHE_NAME"), key);
    assertEquals(CacheGetStatus.MISS, getForKeyInSomeOtherCache.status());

    target.deleteCache(cacheName);
  }

  @Test
  public void shouldFlushCacheContents() {
    String cacheName = UUID.randomUUID().toString();
    String key = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();
    long ttl1HourInSeconds = Duration.ofHours(1).getSeconds();

    target.createCache(cacheName);
    try {
      target.set(cacheName, key, value, ttl1HourInSeconds);
      CacheGetResponse getResponse = target.get(cacheName, key);
      assertEquals(CacheGetStatus.HIT, getResponse.status());
      assertEquals(value, getResponse.string().get());

      // Execute Flush
      target.flushCache(cacheName);

      // Verify that previously set key is now a MISS
      CacheGetResponse getResponseAfterFlush = target.get(cacheName, key);
      assertEquals(CacheGetStatus.MISS, getResponseAfterFlush.status());
    } finally {
      target.deleteCache(cacheName);
    }
  }

  @Test
  public void throwsExceptionWhenClientUsesNegativeDefaultTtl() {
    assertThrows(
        InvalidArgumentException.class,
        () -> SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), -1).build());
  }

  @Test
  public void initializesSdkAndCanHitDataPlaneForUnreachableControlPlane() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(BAD_CONTROL_PLANE_JWT, DEFAULT_TTL_SECONDS).build()) {
      // Unable to hit control plane
      InternalServerException e =
          assertThrows(
              InternalServerException.class,
              () -> client.createCache(UUID.randomUUID().toString()));
      assertTrue(e.getMessage().contains("Unable to reach request endpoint."));

      // But gets a valid response from Data plane
      assertThrows(AuthenticationException.class, () -> client.get("helloCache", "key"));
      assertThrows(AuthenticationException.class, () -> client.set("helloCache", "key", "value"));

      ExecutionException getException =
          assertThrows(ExecutionException.class, () -> client.getAsync("helloCache", "key").get());
      assertTrue(getException.getCause() instanceof AuthenticationException);

      ExecutionException setException =
          assertThrows(
              ExecutionException.class, () -> client.setAsync("helloCache", "key", "value").get());
      assertTrue(setException.getCause() instanceof AuthenticationException);
    }
  }

  @Test
  public void initializesSdkAndCanHitControlPlaneForUnreachableDataPlane() {
    try (SimpleCacheClient client =
        SimpleCacheClient.builder(BAD_DATA_PLANE_JWT, DEFAULT_TTL_SECONDS).build()) {

      // Can reach control plane.
      assertThrows(
          AuthenticationException.class, () -> client.createCache(UUID.randomUUID().toString()));

      // Unable to reach data plane
      assertThrows(InternalServerException.class, () -> client.get("helloCache", "key"));
      assertThrows(InternalServerException.class, () -> client.set("helloCache", "key", "value"));

      ExecutionException getException =
          assertThrows(ExecutionException.class, () -> client.getAsync("helloCache", "key").get());
      assertTrue(getException.getCause() instanceof InternalServerException);
      assertTrue(getException.getMessage().contains("Unable to reach request endpoint."));

      ExecutionException setException =
          assertThrows(
              ExecutionException.class, () -> client.setAsync("helloCache", "key", "value").get());
      assertTrue(setException.getCause() instanceof InternalServerException);
      assertTrue(setException.getMessage().contains("Unable to reach request endpoint."));
    }
  }
}
