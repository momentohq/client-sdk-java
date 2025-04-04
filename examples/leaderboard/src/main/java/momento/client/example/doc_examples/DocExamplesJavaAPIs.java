package momento.client.example.doc_examples;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import momento.sdk.ILeaderboard;
import momento.sdk.LeaderboardClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfigurations;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.DeleteResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.LengthResponse;
import momento.sdk.responses.leaderboard.RemoveElementsResponse;
import momento.sdk.responses.leaderboard.UpsertResponse;

public class DocExamplesJavaAPIs {

  private static final int TOTAL_NUM_ELEMENTS = 2;

  @SuppressWarnings("EmptyTryBlock")
  public static void example_API_InstantiateLeaderboardClient() {
    try (final LeaderboardClient leaderboardClient =
        LeaderboardClient.builder(
                CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                LeaderboardConfigurations.Laptop.latest())
            .build()) {
      // ...
    }
  }

  public static void example_API_CreateLeaderboard(LeaderboardClient leaderboardClient) {
    final ILeaderboard leaderboard;
    try {
      leaderboard = leaderboardClient.leaderboard("cache", "leaderboard");
    } catch (InvalidArgumentException e) {
      throw new RuntimeException("Cache name or leaderboard name is invalid.", e);
    }
  }

  public static void example_API_LeaderboardUpsert(ILeaderboard leaderboard) {
    final Map<Integer, Double> elements =
        Map.of(
            123, 100.0,
            456, 200.0,
            789, 300.0);
    final UpsertResponse response = leaderboard.upsert(elements).join();
    if (response instanceof UpsertResponse.Success) {
      System.out.println("Successfully upserted elements.");
    } else if (response instanceof UpsertResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to call upsert elements into a leaderboard: "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_LeaderboardUpsertPagination(ILeaderboard leaderboard) {
    final Map<Integer, Double> elements =
        IntStream.range(0, TOTAL_NUM_ELEMENTS)
            .boxed()
            .collect(
                Collectors.toMap(i -> i + 1, i -> i * ThreadLocalRandom.current().nextDouble()));

    for (long i = 0; i < elements.size(); i += 8192) {
      final Map<Integer, Double> batch =
          elements.entrySet().stream()
              .skip(i)
              .limit(8192)
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1, HashMap::new));

      final UpsertResponse response = leaderboard.upsert(batch).join();
      if (response instanceof UpsertResponse.Success) {
        System.out.printf("Successfully upserted batch of %d elements%n", batch.size());
      } else if (response instanceof UpsertResponse.Error error) {
        throw new RuntimeException(
            "An error occurred while upserting a batch of elements: " + error.getErrorCode(),
            error);
      }
    }
  }

  public static void example_API_LeaderboardFetchByScore(ILeaderboard leaderboard) {
    // By default, FetchByScore will fetch the elements from the entire score range
    // with zero offset in ascending order. It can return 8192 elements at a time.
    FetchResponse response = leaderboard.fetchByScore().join();
    if (response instanceof FetchResponse.Success success) {
      System.out.println("Successfully fetched elements:");
      for (LeaderboardElement element : success.values()) {
        System.out.printf(
            "id: %d, score: %.2f, rank: %d%n",
            element.getId(), element.getScore(), element.getRank());
      }
    } else if (response instanceof FetchResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
    }

    // Example specifying all FetchByScore options. You can provide any subset of these options
    // to modify your FetchByScore request.
    response = leaderboard.fetchByScore(1.0, 5.0, SortOrder.DESCENDING, 10, 10).join();
    if (response instanceof FetchResponse.Success success) {
      System.out.println("Successfully fetched elements:");
      for (LeaderboardElement element : success.values()) {
        System.out.printf(
            "id: %d, score: %.2f, rank: %d%n",
            element.getId(), element.getScore(), element.getRank());
      }
    } else if (response instanceof FetchResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
    }
  }

  public static void example_API_LeaderboardFetchByScorePagination(ILeaderboard leaderboard) {
    for (int offset = 0; offset < TOTAL_NUM_ELEMENTS; offset += 8192) {
      final FetchResponse response =
          leaderboard.fetchByScore(null, null, null, offset, null).join();
      if (response instanceof FetchResponse.Success success) {
        System.out.println("Successfully fetched elements:");
        for (LeaderboardElement element : success.values()) {
          System.out.printf(
              "id: %d, score: %.2f, rank: %d%n",
              element.getId(), element.getScore(), element.getRank());
        }
      } else if (response instanceof FetchResponse.Error error) {
        throw new RuntimeException(
            "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
      }
    }
  }

  public static void example_API_LeaderboardFetchByRank(ILeaderboard leaderboard) {
    final FetchResponse response = leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING).join();
    if (response instanceof FetchResponse.Success success) {
      System.out.println("Successfully fetched elements:");
      for (LeaderboardElement element : success.values()) {
        System.out.printf(
            "id: %d, score: %.2f, rank: %d%n",
            element.getId(), element.getScore(), element.getRank());
      }
    } else if (response instanceof FetchResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
    }
  }

  public static void example_API_LeaderboardFetchByRankPagination(ILeaderboard leaderboard) {
    for (int rank = 0; rank < TOTAL_NUM_ELEMENTS; rank += 8192) {
      final FetchResponse response =
          leaderboard.fetchByRank(rank, rank + 8192, SortOrder.ASCENDING).join();
      if (response instanceof FetchResponse.Success success) {
        System.out.println("Successfully fetched elements:");
        for (LeaderboardElement element : success.values()) {
          System.out.printf(
              "id: %d, score: %.2f, rank: %d%n",
              element.getId(), element.getScore(), element.getRank());
        }
      } else if (response instanceof FetchResponse.Error error) {
        throw new RuntimeException(
            "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
      }
    }
  }

  public static void example_API_LeaderboardGetRank(ILeaderboard leaderboard) {
    final Set<Integer> ids = Set.of(123, 456, 789);
    final FetchResponse response = leaderboard.getRank(ids, SortOrder.ASCENDING).join();
    if (response instanceof FetchResponse.Success success) {
      System.out.println("Successfully fetched elements:");
      for (LeaderboardElement element : success.values()) {
        System.out.printf(
            "id: %d, score: %.2f, rank: %d%n",
            element.getId(), element.getScore(), element.getRank());
      }
    } else if (response instanceof FetchResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
    }
  }

  public static void example_API_LeaderboardLength(ILeaderboard leaderboard) {
    final LengthResponse response = leaderboard.length().join();
    if (response instanceof LengthResponse.Success success) {
      System.out.printf("Leaderboard length: %d%n", success.length());
    } else if (response instanceof LengthResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to get the length of a leaderboard: "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_LeaderboardRemoveElements(ILeaderboard leaderboard) {
    final Set<Integer> ids = Set.of(123, 456, 789);
    final RemoveElementsResponse response = leaderboard.removeElements(ids).join();
    if (response instanceof RemoveElementsResponse.Success) {
      System.out.println("Successfully removed elements.");
    } else if (response instanceof RemoveElementsResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to remove elements: " + error.getErrorCode(), error);
    }
  }

  @SuppressWarnings("ConstantValue")
  public static void example_API_LeaderboardRemoveElementsPagination(ILeaderboard leaderboard) {
    final List<Integer> ids = List.of(/* your ids */ );
    for (int i = 0; i < ids.size(); i += 8192) {
      final List<Integer> idsSublist = ids.subList(i, Math.min(i + 8192, ids.size()));
      final RemoveElementsResponse response = leaderboard.removeElements(idsSublist).join();
      if (response instanceof RemoveElementsResponse.Success) {
        System.out.println("Successfully removed elements.");
      } else if (response instanceof RemoveElementsResponse.Error error) {
        throw new RuntimeException(
            "An error occurred while attempting to remove elements: " + error.getErrorCode(),
            error);
      }
    }
  }

  public static void example_API_LeaderboardDelete(ILeaderboard leaderboard) {
    final momento.sdk.responses.leaderboard.DeleteResponse response = leaderboard.delete().join();
    if (response instanceof DeleteResponse.Success) {
      System.out.println("Successfully deleted leaderboard.");
    } else if (response instanceof DeleteResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to delete the leaderboard: " + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_LeaderboardGetCompetitionRank(ILeaderboard leaderboard) {
    final Set<Integer> ids = Set.of(123, 456, 789);
    final FetchResponse response = leaderboard.getCompetitionRank(ids, SortOrder.ASCENDING).join();
    if (response instanceof FetchResponse.Success success) {
      System.out.println("Successfully fetched elements:");
      for (LeaderboardElement element : success.values()) {
        System.out.printf(
            "id: %d, score: %.2f, rank: %d%n",
            element.getId(), element.getScore(), element.getRank());
      }
    } else if (response instanceof FetchResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to fetch elements: " + error.getErrorCode(), error);
    }
  }

  public static void main(String[] args) {
    try (final LeaderboardClient leaderboardClient =
        LeaderboardClient.builder(
                CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                LeaderboardConfigurations.Laptop.latest())
            .build()) {
      example_API_InstantiateLeaderboardClient();
      example_API_CreateLeaderboard(leaderboardClient);

      final ILeaderboard leaderboard = leaderboardClient.leaderboard("test-cache", "leaderboard");
      example_API_LeaderboardUpsert(leaderboard);
      example_API_LeaderboardUpsertPagination(leaderboard);
      example_API_LeaderboardFetchByScore(leaderboard);
      example_API_LeaderboardFetchByScorePagination(leaderboard);
      example_API_LeaderboardFetchByRank(leaderboard);
      example_API_LeaderboardFetchByRankPagination(leaderboard);
      example_API_LeaderboardGetRank(leaderboard);
      example_API_LeaderboardLength(leaderboard);
      example_API_LeaderboardRemoveElements(leaderboard);
      example_API_LeaderboardRemoveElementsPagination(leaderboard);
      example_API_LeaderboardDelete(leaderboard);
      example_API_LeaderboardGetCompetitionRank(leaderboard);
    }
  }
}
