package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.CacheInfo;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.SigningKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class SimpleCacheControlPlaneTest extends BaseTestClass {

  private static final int DEFAULT_TTL_SECONDS = 60;

  private SimpleCacheClient target;
  private String authToken;

  @BeforeEach
  void setup() {
    authToken = System.getenv("TEST_AUTH_TOKEN");
    target = SimpleCacheClient.builder(authToken, DEFAULT_TTL_SECONDS).build();
  }

  @AfterEach
  void tearDown() {
    target.close();
  }

  @Test
  public void createListRevokeSigningKeyWorks() {
    CreateSigningKeyResponse createSigningKeyResponse = target.createSigningKey(30);
    ListSigningKeysResponse listSigningKeysResponse = target.listSigningKeys(null);
    assertTrue(
        listSigningKeysResponse.signingKeys().stream()
            .map(SigningKey::getKeyId)
            .anyMatch(keyId -> createSigningKeyResponse.getKeyId().equals(keyId)));
    target.revokeSigningKey(createSigningKeyResponse.getKeyId());
    listSigningKeysResponse = target.listSigningKeys(null);
    assertFalse(
        listSigningKeysResponse.signingKeys().stream()
            .map(SigningKey::getKeyId)
            .anyMatch(keyId -> createSigningKeyResponse.getKeyId().equals(keyId)));
  }

  @Test
  public void throwsAlreadyExistsWhenCreatingExistingCache() {
    final String existingCache = System.getenv("TEST_CACHE_NAME");

    final CreateCacheResponse response = target.createCache(existingCache);
    assertThat(response).isInstanceOf(CreateCacheResponse.Error.class);
    assertThat(((CreateCacheResponse.Error) response))
        .hasCauseInstanceOf(AlreadyExistsException.class);
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownCache() {
    final DeleteCacheResponse response = target.deleteCache(randomString("name"));
    assertThat(response).isInstanceOf(DeleteCacheResponse.Error.class);
    assertThat(((DeleteCacheResponse.Error) response)).hasCauseInstanceOf(NotFoundException.class);
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
  public void returnsBadRequestForEmptyCacheName() {
    final CreateCacheResponse response = target.createCache("      ");
    assertThat(response).isInstanceOf(CreateCacheResponse.Error.class);
    assertThat(((CreateCacheResponse.Error) response))
        .hasCauseInstanceOf(BadRequestException.class);
  }

  @Test
  public void throwsValidationExceptionForNullCacheName() {
    final CreateCacheResponse createResponse = target.createCache(null);
    assertThat(createResponse).isInstanceOf(CreateCacheResponse.Error.class);
    assertThat(((CreateCacheResponse.Error) createResponse))
        .hasCauseInstanceOf(InvalidArgumentException.class);

    final DeleteCacheResponse deleteResponse = target.deleteCache(null);
    assertThat(deleteResponse).isInstanceOf(DeleteCacheResponse.Error.class);
    assertThat(((DeleteCacheResponse.Error) deleteResponse))
        .hasCauseInstanceOf(InvalidArgumentException.class);
  }

  @Test
  public void deleteSucceeds() {
    final String cacheName = randomString("name");

    target.createCache(cacheName);

    final CreateCacheResponse secondCreate = target.createCache(cacheName);
    assertThat(secondCreate).isInstanceOf(CreateCacheResponse.Error.class);
    assertThat(((CreateCacheResponse.Error) secondCreate))
        .hasCauseInstanceOf(AlreadyExistsException.class);

    target.deleteCache(cacheName);

    final DeleteCacheResponse secondDelete = target.deleteCache(cacheName);
    assertThat(secondDelete).isInstanceOf(DeleteCacheResponse.Error.class);
    assertThat(((DeleteCacheResponse.Error) secondDelete))
        .hasCauseInstanceOf(NotFoundException.class);
  }

  @Test
  public void returnsErrorForBadToken() {
    final String cacheName = randomString("name");
    final String badToken =
        "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJpbnRlZ3JhdGlvbiIsImNwIjoiY29udHJvbC5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b"
            + "2hxLmNvbSIsImMiOiJjYWNoZS5jZWxsLWFscGhhLWRldi5wcmVwcm9kLmEubW9tZW50b2hxLmNvbSJ9.gdghdjjfjyehhdkkkskskmml"
            + "s76573jnajhjjjhjdhnndy";

    try (final SimpleCacheClient client = SimpleCacheClient.builder(badToken, 10).build()) {
      final CreateCacheResponse createResponse = client.createCache(cacheName);
      assertThat(createResponse).isInstanceOf(CreateCacheResponse.Error.class);
      assertThat(((CreateCacheResponse.Error) createResponse))
          .hasCauseInstanceOf(AuthenticationException.class);

      final DeleteCacheResponse deleteResponse = client.deleteCache(cacheName);
      assertThat(deleteResponse).isInstanceOf(DeleteCacheResponse.Error.class);
      assertThat(((DeleteCacheResponse.Error) deleteResponse))
          .hasCauseInstanceOf(AuthenticationException.class);

      assertThrows(AuthenticationException.class, () -> client.listCaches(Optional.empty()));
    }
  }

  @Test
  public void throwsInvalidArgumentForZeroRequestTimeout() {
    assertThrows(
        InvalidArgumentException.class,
        () ->
            SimpleCacheClient.builder(authToken, DEFAULT_TTL_SECONDS)
                .requestTimeout(Duration.ofMillis(0))
                .build());
  }

  @Test
  public void throwsInvalidArgumentForNegativeRequestTimeout() {
    assertThrows(
        InvalidArgumentException.class,
        () ->
            SimpleCacheClient.builder(authToken, DEFAULT_TTL_SECONDS)
                .requestTimeout(Duration.ofMillis(-1))
                .build());
  }

  @Test
  public void throwsInvalidArgumentForNullRequestTimeout() {
    assertThrows(
        InvalidArgumentException.class,
        () ->
            SimpleCacheClient.builder(authToken, DEFAULT_TTL_SECONDS).requestTimeout(null).build());
  }
}
