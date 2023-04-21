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
import momento.sdk.responses.CreateCacheResponse;
import momento.sdk.responses.CreateSigningKeyResponse;
import momento.sdk.responses.DeleteCacheResponse;
import momento.sdk.responses.ListCachesResponse;
import momento.sdk.responses.ListSigningKeysResponse;
import momento.sdk.responses.RevokeSigningKeyResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class CacheControlPlaneTest extends BaseTestClass {

  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);

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
        target.createSigningKey(Duration.ofMinutes(30)).join();
    assertThat(createSigningKeyResponse).isInstanceOf(CreateSigningKeyResponse.Success.class);
    final String keyId = ((CreateSigningKeyResponse.Success) createSigningKeyResponse).getKeyId();

    assertThat(target.listSigningKeys())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListSigningKeysResponse.Success.class))
        .satisfies(
            success ->
                assertThat(success.signingKeys()).anyMatch(sk -> sk.getKeyId().equals(keyId)));

    assertThat(target.revokeSigningKey(keyId))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(RevokeSigningKeyResponse.Success.class);

    assertThat(target.listSigningKeys())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(ListSigningKeysResponse.Success.class))
        .satisfies(
            success ->
                assertThat(success.signingKeys()).noneMatch(sk -> sk.getKeyId().equals(keyId)));
  }

  @Test
  public void throwsAlreadyExistsWhenCreatingExistingCache() {
    final String existingCache = System.getenv("TEST_CACHE_NAME");

    assertThat(target.createCache(existingCache))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownCache() {
    assertThat(target.deleteCache(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
  }

  @Test
  public void listsCachesHappyPath() {
    final String cacheName = randomString("name");

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CreateCacheResponse.Success.class);

    try {
      assertThat(target.listCaches())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListCachesResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getCaches()).anyMatch(ci -> ci.name().equals(cacheName)));
    } finally {
      // cleanup
      assertThat(target.deleteCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(DeleteCacheResponse.Success.class);
    }
  }

  @Test
  public void returnsBadRequestForEmptyCacheName() {
    assertThat(target.createCache("      "))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(BadRequestException.class));
  }

  @Test
  public void throwsValidationExceptionForNullCacheName() {
    assertThat(target.createCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.deleteCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteSucceeds() {
    final String cacheName = randomString("name");

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CreateCacheResponse.Success.class);

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));

    assertThat(target.deleteCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DeleteCacheResponse.Success.class);

    assertThat(target.deleteCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteCacheResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(NotFoundException.class));
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
      assertThat(client.createCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CreateCacheResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.deleteCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(DeleteCacheResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.listCaches())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(ListCachesResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));
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
