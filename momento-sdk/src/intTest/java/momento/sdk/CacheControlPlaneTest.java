package momento.sdk;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.exceptions.AuthenticationException;
import momento.sdk.exceptions.BadRequestException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.exceptions.NotFoundException;
import momento.sdk.messages.CreateCacheResponse;
import momento.sdk.messages.CreateSigningKeyResponse;
import momento.sdk.messages.DeleteCacheResponse;
import momento.sdk.messages.ListCachesResponse;
import momento.sdk.messages.ListSigningKeysResponse;
import momento.sdk.messages.RevokeSigningKeyResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class CacheControlPlaneTest extends BaseTestClass {

  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

  private final CredentialProvider credentialProvider =
      CredentialProvider.fromEnvVar("TEST_AUTH_TOKEN");
  private CacheClient target;

  @BeforeEach
  void setup() {
    target =
        CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
            .build();
  }

  @AfterEach
  void tearDown() {
    target.close();
  }

  @Test
  public void createListRevokeSigningKeyWorks() {
    final CreateSigningKeyResponse createSigningKeyResponse =
        target.createSigningKey(Duration.ofMinutes(30));
    assertThat(createSigningKeyResponse).isInstanceOf(CreateSigningKeyResponse.Success.class);
    final String keyId = ((CreateSigningKeyResponse.Success) createSigningKeyResponse).getKeyId();

    final ListSigningKeysResponse listSigningKeysResponse = target.listSigningKeys();
    assertThat(listSigningKeysResponse).isInstanceOf(ListSigningKeysResponse.Success.class);
    assertThat(((ListSigningKeysResponse.Success) listSigningKeysResponse).signingKeys())
        .anyMatch(signingKey -> signingKey.getKeyId().equals(keyId));

    final RevokeSigningKeyResponse revokeResponse = target.revokeSigningKey(keyId);
    assertThat(revokeResponse).isInstanceOf(RevokeSigningKeyResponse.Success.class);

    final ListSigningKeysResponse listAfterRevokeResponse = target.listSigningKeys();
    assertThat(listAfterRevokeResponse).isInstanceOf(ListSigningKeysResponse.Success.class);
    assertThat(((ListSigningKeysResponse.Success) listAfterRevokeResponse).signingKeys())
        .noneMatch(signingKey -> signingKey.getKeyId().equals(keyId));
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
  public void listsCachesHappyPath() {
    final String cacheName = randomString("name");
    target.createCache(cacheName);
    try {
      final ListCachesResponse response = target.listCaches();
      assertThat(response).isInstanceOf(ListCachesResponse.Success.class);
      assertThat(((ListCachesResponse.Success) response).getCaches())
          .anyMatch(cacheInfo -> cacheInfo.name().equals(cacheName));
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
    final CredentialProvider badTokenProvider = CredentialProvider.fromString(badToken);

    try (final CacheClient client =
        CacheClient.builder(
                badTokenProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
            .build()) {
      final CreateCacheResponse createResponse = client.createCache(cacheName);
      assertThat(createResponse).isInstanceOf(CreateCacheResponse.Error.class);
      assertThat(((CreateCacheResponse.Error) createResponse))
          .hasCauseInstanceOf(AuthenticationException.class);

      final DeleteCacheResponse deleteResponse = client.deleteCache(cacheName);
      assertThat(deleteResponse).isInstanceOf(DeleteCacheResponse.Error.class);
      assertThat(((DeleteCacheResponse.Error) deleteResponse))
          .hasCauseInstanceOf(AuthenticationException.class);

      final ListCachesResponse listCachesResponse = client.listCaches();
      assertThat(listCachesResponse).isInstanceOf(ListCachesResponse.Error.class);
      assertThat((ListCachesResponse.Error) listCachesResponse)
          .hasCauseInstanceOf(AuthenticationException.class);
    }
  }

  @Test
  public void throwsInvalidArgumentForZeroRequestTimeout() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
                    .setDeadline(Duration.ofMillis(0))
                    .build());
  }

  @Test
  public void throwsInvalidArgumentForNegativeRequestTimeout() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
                    .setDeadline(Duration.ofMillis(-1))
                    .build());
  }

  @Test
  public void throwsInvalidArgumentForNullRequestTimeout() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL_SECONDS)
                    .setDeadline(null)
                    .build());
  }
}
