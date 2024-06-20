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
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import momento.sdk.responses.cache.control.CacheListResponse;
import momento.sdk.responses.cache.signing.SigningKeyCreateResponse;
import momento.sdk.responses.cache.signing.SigningKeyListResponse;
import momento.sdk.responses.cache.signing.SigningKeyRevokeResponse;
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
    final SigningKeyCreateResponse signingKeyCreateResponse =
        target.createSigningKey(Duration.ofMinutes(30)).join();
    assertThat(signingKeyCreateResponse).isInstanceOf(SigningKeyCreateResponse.Success.class);
    final String keyId = ((SigningKeyCreateResponse.Success) signingKeyCreateResponse).getKeyId();

    assertThat(target.listSigningKeys())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SigningKeyListResponse.Success.class))
        .satisfies(
            success ->
                assertThat(success.signingKeys()).anyMatch(sk -> sk.getKeyId().equals(keyId)));

    assertThat(target.revokeSigningKey(keyId))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(SigningKeyRevokeResponse.Success.class);

    assertThat(target.listSigningKeys())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(SigningKeyListResponse.Success.class))
        .satisfies(
            success ->
                assertThat(success.signingKeys()).noneMatch(sk -> sk.getKeyId().equals(keyId)));
  }

  @Test
  public void throwsAlreadyExistsWhenCreatingExistingCache() {
    final String existingCache = System.getenv("TEST_CACHE_NAME");

    assertThat(target.createCache(existingCache))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));
  }

  @Test
  public void returnsNotFoundWhenDeletingUnknownCache() {
    assertThat(target.deleteCache(randomString("name")))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void listsCachesHappyPath() {
    final String cacheName = randomString("name");

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheCreateResponse.Success.class);

    try {
      assertThat(target.listCaches())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheListResponse.Success.class))
          .satisfies(
              success ->
                  assertThat(success.getCaches()).anyMatch(ci -> ci.name().equals(cacheName)));
    } finally {
      // cleanup
      assertThat(target.deleteCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .isInstanceOf(CacheDeleteResponse.Success.class);
    }
  }

  @Test
  public void returnsBadRequestForEmptyCacheName() {
    assertThat(target.createCache("      "))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(BadRequestException.class));
  }

  @Test
  public void throwsValidationExceptionForNullCacheName() {
    assertThat(target.createCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(target.deleteCache(null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteSucceeds() {
    final String cacheName = randomString("name");

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheCreateResponse.Success.class);

    assertThat(target.createCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(AlreadyExistsException.class));

    assertThat(target.deleteCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(CacheDeleteResponse.Success.class);

    assertThat(target.deleteCache(cacheName))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(CacheDeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
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
          .asInstanceOf(InstanceOfAssertFactories.type(CacheCreateResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.deleteCache(cacheName))
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheDeleteResponse.Error.class))
          .satisfies(error -> assertThat(error).hasCauseInstanceOf(AuthenticationException.class));

      assertThat(client.listCaches())
          .succeedsWithin(FIVE_SECONDS)
          .asInstanceOf(InstanceOfAssertFactories.type(CacheListResponse.Error.class))
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
                        credentialProvider,
                        Configurations.Laptop.latest().withTimeout(Duration.ofMillis(0)),
                        DEFAULT_TTL_SECONDS)
                    .build());
  }

  @Test
  public void throwsInvalidArgumentForNegativeRequestTimeout() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider,
                        Configurations.Laptop.latest().withTimeout(Duration.ofMillis(-1)),
                        DEFAULT_TTL_SECONDS)
                    .build());
  }

  @Test
  public void throwsInvalidArgumentForNullRequestTimeout() {
    //noinspection resource
    assertThatExceptionOfType(InvalidArgumentException.class)
        .isThrownBy(
            () ->
                CacheClient.builder(
                        credentialProvider,
                        Configurations.Laptop.latest().withTimeout(null),
                        DEFAULT_TTL_SECONDS)
                    .build());
  }
}
