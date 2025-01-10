package momento.sdk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.RemoveElementsResponse;
import momento.sdk.responses.leaderboard.UpsertResponse;

public class Leaderboard implements ILeaderboard {
  private final String cacheName;
  private final String leaderboardName;

  private final LeaderboardDataClient leaderboardDataClient;

  Leaderboard(LeaderboardDataClient dataClient, String cacheName, String leaderboardName) {
    this.cacheName = cacheName;
    this.leaderboardName = leaderboardName;
    this.leaderboardDataClient = dataClient;
  }

  @Override
  public CompletableFuture<UpsertResponse> upsert(@Nonnull Map<Integer, Double> elements) {
    return leaderboardDataClient.upsert(cacheName, leaderboardName, elements);
  }

  @Override
  public CompletableFuture<FetchResponse> fetchByRank(
      int startRank, int endRank, @Nonnull SortOrder order) {
    return leaderboardDataClient.fetchByRank(cacheName, leaderboardName, startRank, endRank, order);
  }

  @Override
  public CompletableFuture<RemoveElementsResponse> removeElements(@Nonnull Iterable<Integer> ids) {
    return leaderboardDataClient.removeElements(cacheName, leaderboardName, ids);
  }
}
