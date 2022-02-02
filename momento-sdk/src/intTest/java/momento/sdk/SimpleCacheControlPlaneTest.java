package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SimpleCacheControlPlaneTest extends BaseTestClass {

  private static final int DEFAULT_TTL_SECONDS = 60;

  private SimpleCacheClient target;

  @BeforeEach
  void setup() {
    target =
        SimpleCacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void tearDown() {
    target.close();
  }

  @Test
  public void throwsAlreadyExistsWhenCreatingExistingCache() {
    String existingCache = System.getenv("TEST_CACHE_NAME");
    assertThrows(AlreadyExistsException.class, () -> target.createCache(existingCache));
  }

  @Test
  public void throwsNotFoundWhenDeletingUnknownCache() {
    String doesNotExistCache = UUID.randomUUID().toString();
    assertThrows(NotFoundException.class, () -> target.deleteCache(doesNotExistCache));
  }

  @Test
  public void listsCachesSuccessfullyHandlesNullToken() {
    String cacheName = UUID.randomUUID().toString();
    target.createCache(cacheName);
    try {
      ListCachesResponse response = target.listCaches(null);
      assertTrue(response.caches().size() >= 1);
      assertTrue(
          response.caches().stream()
              .map(CacheInfo::name)
              .collect(Collectors.toSet())
              .contains(cacheName));
      assertFalse(response.nextPageToken().isPresent());
    } finally {
      // cleanup
      target.deleteCache(cacheName);
    }
  }

  @Test
  public void listsCachesSuccessfullyHandlesEmptyToken() {
    String cacheName = UUID.randomUUID().toString();
    target.createCache(cacheName);
    try {
      ListCachesResponse response = target.listCaches(Optional.empty());
      assertTrue(response.caches().size() >= 1);
      assertTrue(
          response.caches().stream()
              .map(CacheInfo::name)
              .collect(Collectors.toSet())
              .contains(cacheName));
      assertFalse(response.nextPageToken().isPresent());
    } finally {
      // cleanup
      target.deleteCache(cacheName);
    }
  }

  @Test
  public void throwsInvalidArgumentForEmptyCacheName() {
    assertThrows(BadRequestException.class, () -> target.createCache("     "));
  }

  @Test
  public void throwsValidationExceptionForNullCacheName() {
    assertThrows(InvalidArgumentException.class, () -> target.createCache(null));
    assertThrows(InvalidArgumentException.class, () -> target.deleteCache(null));
  }

  @Test
  public void deleteSucceeds() {
    String cacheName = UUID.randomUUID().toString();
    target.createCache(cacheName);
    assertThrows(AlreadyExistsException.class, () -> target.createCache(cacheName));
    target.deleteCache(cacheName);
    assertThrows(NotFoundException.class, () -> target.deleteCache(cacheName));
  }

  @Test
  public void throwsAuthenticationExceptionForBadToken() {
    String cacheName = UUID.randomUUID().toString();
    String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    SimpleCacheClient target = SimpleCacheClient.builder(badToken, 10).build();
    assertThrows(AuthenticationException.class, () -> target.createCache(cacheName));

    assertThrows(AuthenticationException.class, () -> target.deleteCache(cacheName));
    assertThrows(AuthenticationException.class, () -> target.listCaches(Optional.empty()));
  }
}
