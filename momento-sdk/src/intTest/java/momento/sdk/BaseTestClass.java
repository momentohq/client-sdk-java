package momento.sdk;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import org.junit.jupiter.api.BeforeAll;

class BaseTestClass {

  public static final Duration FIVE_SECONDS = Duration.ofSeconds(5);

  public static final CredentialProvider credentialProvider =
      CredentialProvider.fromEnvVar("TEST_AUTH_TOKEN");

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
    ensureTestCacheExists();
  }

  private static void ensureTestCacheExists() {
    try (CacheClient client =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.latest(), Duration.ofSeconds(10))
            .build()) {
      client.createCache(System.getenv("TEST_CACHE_NAME")).join();
    }
  }
}
