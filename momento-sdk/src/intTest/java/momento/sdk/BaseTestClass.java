package momento.sdk;

import java.time.Duration;
import java.util.UUID;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configurations;
import org.junit.jupiter.api.BeforeAll;

public class BaseTestClass {
  public static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  public static final Duration TEN_SECONDS = Duration.ofSeconds(10);

  public static final CredentialProvider credentialProvider =
      CredentialProvider.fromEnvVar("TEST_AUTH_TOKEN");

  @BeforeAll
  static void beforeAll() {
    if (System.getenv("TEST_AUTH_TOKEN") == null) {
      throw new IllegalArgumentException(
          "Integration tests require TEST_AUTH_TOKEN env var; see README for more details.");
    }
  }

  private static void ensureTestCacheExists(String cacheName) {
    try (CacheClient client =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.latest(), TEN_SECONDS)
            .build()) {
      client.createCache(cacheName).join();
    }
  }

  public static void cleanupTestCache(String cacheName) {
    try (CacheClient client =
        CacheClient.builder(
                credentialProvider, Configurations.Laptop.latest(), TEN_SECONDS)
            .build()) {
      client.deleteCache(cacheName).join();
    }
  }

  public static String testCacheName() {
    return "java-integration-test-default-" + UUID.randomUUID().toString();
  }

  public static String testStoreName() {
    return "java-integration-test-default-" + UUID.randomUUID().toString();
  }
}
