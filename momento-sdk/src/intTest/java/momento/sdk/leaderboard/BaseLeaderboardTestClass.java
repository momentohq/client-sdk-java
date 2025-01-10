package momento.sdk.leaderboard;

import java.time.Duration;
import java.util.UUID;
import momento.sdk.CacheClient;
import momento.sdk.LeaderboardClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.config.Configurations;
import momento.sdk.config.LeaderboardConfiguration;
import momento.sdk.config.LeaderboardConfigurations;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheDeleteResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class BaseLeaderboardTestClass {
  protected static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  protected static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
  protected static CredentialProvider credentialProvider;

  protected static CacheClient cacheClient;
  protected static LeaderboardClient leaderboardClient;
  protected static String cacheName;

  @BeforeAll
  static void beforeAll() {
    credentialProvider = CredentialProvider.fromEnvVar("MOMENTO_API_KEY");

    final Configuration config = Configurations.Laptop.latest();

    cacheClient = CacheClient.builder(credentialProvider, config, DEFAULT_TTL_SECONDS).build();

    final LeaderboardConfiguration leaderboardConfig = LeaderboardConfigurations.Laptop.latest();
    leaderboardClient = LeaderboardClient.builder(credentialProvider, leaderboardConfig).build();

    cacheName = testCacheName();
    ensureTestCacheExists(cacheName);
  }

  @AfterAll
  static void afterAll() {
    cleanupTestCache(cacheName);
    cacheClient.close();
    leaderboardClient.close();
  }

  protected static void ensureTestCacheExists(String cacheName) {
    CacheCreateResponse response = cacheClient.createCache(cacheName).join();
    if (response instanceof CacheCreateResponse.Error) {
      throw new RuntimeException(
          "Failed to test create cache: " + ((CacheCreateResponse.Error) response).getMessage());
    }
  }

  public static void cleanupTestCache(String cacheName) {
    CacheDeleteResponse response = cacheClient.deleteCache(cacheName).join();
    if (response instanceof CacheDeleteResponse.Error) {
      throw new RuntimeException(
          "Failed to test delete cache: " + ((CacheDeleteResponse.Error) response).getMessage());
    }
  }

  public static String testCacheName() {
    return "java-integration-test-default-" + UUID.randomUUID();
  }
}
