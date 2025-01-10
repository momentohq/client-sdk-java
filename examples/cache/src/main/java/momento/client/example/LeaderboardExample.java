package momento.client.example;

import momento.sdk.CacheClient;
import momento.sdk.ILeaderboard;
import momento.sdk.LeaderboardClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.auth.EnvVarCredentialProvider;
import momento.sdk.config.Configurations;
import momento.sdk.config.LeaderboardConfigurations;
import momento.sdk.exceptions.AlreadyExistsException;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.cache.GetResponse;
import momento.sdk.responses.cache.control.CacheCreateResponse;
import momento.sdk.responses.cache.control.CacheInfo;
import momento.sdk.responses.cache.control.CacheListResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.UpsertResponse;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class LeaderboardExample {

  private static final String API_KEY_ENV_VAR = "MOMENTO_API_KEY";
  private static final Duration DEFAULT_ITEM_TTL = Duration.ofSeconds(60);

  private static final String CACHE_NAME = "cache";
  private static final String LEADERBOARD_NAME = "leaderboard";

  public static void main(String[] args) {
    final CredentialProvider credentialProvider = new EnvVarCredentialProvider(API_KEY_ENV_VAR);

    try (final CacheClient cacheClient =
             CacheClient.create(credentialProvider, Configurations.Laptop.latest(), DEFAULT_ITEM_TTL);
         final LeaderboardClient leaderboardClient =
             LeaderboardClient.builder(credentialProvider, LeaderboardConfigurations.Laptop.latest()).build()) {

      cacheClient.createCache(CACHE_NAME).join();

      final ILeaderboard leaderboard = leaderboardClient.leaderboard(CACHE_NAME, LEADERBOARD_NAME);

      final Map<Integer, Double> elements = new HashMap<>();
      elements.put(1, 1.0);
      elements.put(2, 2.0);
      elements.put(3, 3.0);

      final UpsertResponse upsertResponse = leaderboard.upsert(elements).join();
      if (upsertResponse instanceof UpsertResponse.Error error) {
        System.out.println("Error during upsert: " + error.getMessage());
      }

      final FetchResponse fetchResponse = leaderboard.fetchByRank(0, 2, SortOrder.ASCENDING).join();
      if (fetchResponse instanceof FetchResponse.Success success) {
        for (LeaderboardElement element : success.elementsList()) {
          System.out.printf("Rank: %d, ID: %d, Score: %.2f%n",
              element.getRank(),
              element.getId(),
              element.getScore());
        }
      } else if (fetchResponse instanceof FetchResponse.Error error) {
        System.out.println("Error during fetch: " + error.getMessage());
      }
    }
  }
}
