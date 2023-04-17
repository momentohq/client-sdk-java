package momento.sdk;

import java.time.Duration;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.exceptions.AlreadyExistsException;
import org.junit.jupiter.api.BeforeAll;

class BaseTestClass {

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
    final CredentialProvider credentialProvider = CredentialProvider.fromEnvVar("TEST_AUTH_TOKEN");
    try (CacheClient client =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.Latest(), Duration.ofSeconds(10))
            .build()) {
      client.createCache(System.getenv("TEST_CACHE_NAME"));
    } catch (AlreadyExistsException e) {
      // do nothing. Cache already exists.
    }
  }
}
