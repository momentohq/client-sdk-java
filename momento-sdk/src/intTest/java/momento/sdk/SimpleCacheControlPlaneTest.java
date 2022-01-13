package momento.sdk;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import java.util.stream.Collectors;
import momento.sdk.exceptions.CacheAlreadyExistsException;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.PermissionDeniedException;
import momento.sdk.exceptions.ValidationException;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.ListCachesRequest;
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
    assertThrows(CacheAlreadyExistsException.class, () -> target.createCache(existingCache));
  }

  @Test
  public void throwsNotFoundWhenDeletingUnknownCache() {
    String doesNotExistCache = UUID.randomUUID().toString();
    assertThrows(CacheNotFoundException.class, () -> target.deleteCache(doesNotExistCache));
  }

  @Test
  public void listsCachesSuccessfully() {
    String cacheName = UUID.randomUUID().toString();
    target.createCache(cacheName);
    try {
      ListCachesResponse response = target.listCaches(ListCachesRequest.builder().build());
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
    assertThrows(InvalidArgumentException.class, () -> target.createCache("     "));
  }

  @Test
  public void throwsValidationExceptionForNullCacheName() {
    assertThrows(ValidationException.class, () -> target.createCache(null));
    assertThrows(ValidationException.class, () -> target.deleteCache(null));
  }

  @Test
  public void deleteSucceeds() {
    String cacheName = UUID.randomUUID().toString();
    target.createCache(cacheName);
    assertThrows(CacheAlreadyExistsException.class, () -> target.createCache(cacheName));
    target.deleteCache(cacheName);
    assertThrows(CacheNotFoundException.class, () -> target.deleteCache(cacheName));
  }

  @Test
  public void throwsPemissionDeniedForBadToken() {
    String cacheName = UUID.randomUUID().toString();
    String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmmls76573jnajhjjjhjdhnndy";
    SimpleCacheClient target = SimpleCacheClient.builder(badToken, 10).build();
    assertThrows(PermissionDeniedException.class, () -> target.createCache(cacheName));

    assertThrows(PermissionDeniedException.class, () -> target.deleteCache(cacheName));
    assertThrows(
        PermissionDeniedException.class,
        () -> target.listCaches(ListCachesRequest.builder().build()));
  }
}
