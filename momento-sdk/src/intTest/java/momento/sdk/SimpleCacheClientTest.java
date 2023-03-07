package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.InternalServerException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.CacheGetResponse;
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
    final String cacheName = UUID.randomUUID().toString();
    final String alternateCacheName = UUID.randomUUID().toString();
    final String key = UUID.randomUUID().toString();
    final String value = UUID.randomUUID().toString();

    target.createCache(cacheName);
    target.createCache(alternateCacheName);
    try {
      target.set(cacheName, key, value);

      final CacheGetResponse getResponse = target.get(cacheName, key);
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).string()).isEqualTo(value);

      target.delete(cacheName, key);
      final CacheGetResponse getAfterDeleteResponse = target.get(cacheName, key);
      assertThat(getAfterDeleteResponse).isInstanceOf(CacheGetResponse.Miss.class);

      final CacheGetResponse getForKeyInSomeOtherCache = target.get(alternateCacheName, key);
      assertThat(getForKeyInSomeOtherCache).isInstanceOf(CacheGetResponse.Miss.class);
    } finally {
      target.deleteCache(cacheName);
      target.deleteCache(alternateCacheName);
    }
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
      final CacheGetResponse getResponse = target.get(cacheName, key);
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Hit.class);
      assertThat(((CacheGetResponse.Hit) getResponse).string()).isEqualTo(value);

      // Execute Flush
      target.flushCache(cacheName);

      // Verify that previously set key is now a MISS
      final CacheGetResponse getResponseAfterFlush = target.get(cacheName, key);
      assertThat(getResponseAfterFlush).isInstanceOf(CacheGetResponse.Miss.class);
    } finally {
      target.deleteCache(cacheName);
    }
  }

  @Test
  public void shouldThrowNotFoundWhenCacheToFlushDoesNotExist() {
    assertThrows(NotFoundException.class, () -> target.flushCache("non-existent-cache"));
  }

  @Test
  public void shouldThrowIllegalArgWhenCacheNameToFlushIsInvalid() {
    assertThrows(InvalidArgumentException.class, () -> target.flushCache(null));
  }

  @Test
  public void throwsExceptionWhenClientUsesNegativeDefaultTtl() {
    assertThrows(
        InvalidArgumentException.class,
        () -> SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), -1).build());
  }

  @Test
  public void initializesSdkAndCanHitDataPlaneForUnreachableControlPlane() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(BAD_CONTROL_PLANE_JWT, DEFAULT_TTL_SECONDS).build()) {
      // Unable to hit control plane
      final InternalServerException e =
          assertThrows(
              InternalServerException.class,
              () -> client.createCache(UUID.randomUUID().toString()));
      assertTrue(e.getMessage().contains("Unable to reach request endpoint."));

      // But gets a valid response from Data plane
      final CacheGetResponse getResponse = client.get("helloCache", "key");
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) getResponse).exception())
          .isInstanceOf(AuthenticationException.class);

      assertThrows(AuthenticationException.class, () -> client.set("helloCache", "key", "value"));

      final CacheGetResponse asyncGetResponse = client.getAsync("helloCache", "key").join();
      assertThat(asyncGetResponse).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) asyncGetResponse).exception())
          .isInstanceOf(AuthenticationException.class);

      final ExecutionException setException =
          assertThrows(
              ExecutionException.class, () -> client.setAsync("helloCache", "key", "value").get());
      assertTrue(setException.getCause() instanceof AuthenticationException);
    }
  }

  @Test
  public void initializesSdkAndCanHitControlPlaneForUnreachableDataPlane() {
    try (final SimpleCacheClient client =
        SimpleCacheClient.builder(BAD_DATA_PLANE_JWT, DEFAULT_TTL_SECONDS).build()) {

      // Can reach control plane.
      assertThrows(
          AuthenticationException.class, () -> client.createCache(UUID.randomUUID().toString()));

      // Unable to reach data plane
      final CacheGetResponse getResponse = client.get("helloCache", "key");
      assertThat(getResponse).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) getResponse).exception())
          .isInstanceOf(InternalServerException.class);

      assertThrows(InternalServerException.class, () -> client.set("helloCache", "key", "value"));

      final CacheGetResponse response = client.getAsync("helloCache", "key").join();
      assertThat(response).isInstanceOf(CacheGetResponse.Error.class);
      assertThat(((CacheGetResponse.Error) response).exception())
          .isInstanceOf(InternalServerException.class)
          .hasMessageContaining("Unable to reach request endpoint.");

      final ExecutionException setException =
          assertThrows(
              ExecutionException.class, () -> client.setAsync("helloCache", "key", "value").get());
      assertTrue(setException.getCause() instanceof InternalServerException);
      assertTrue(setException.getMessage().contains("Unable to reach request endpoint."));
    }
  }
}
