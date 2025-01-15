package momento.sdk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.DeleteResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LengthResponse;
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
  public CompletableFuture<FetchResponse> fetchByScore(
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    return leaderboardDataClient.fetchByScore(
        cacheName, leaderboardName, minScore, maxScore, order, offset, count);
  }

  @Override
  public CompletableFuture<FetchResponse> fetchByScore(
      @Nullable Double minScore, @Nullable Double maxScore, @Nullable SortOrder order) {
    return leaderboardDataClient.fetchByScore(
        cacheName, leaderboardName, minScore, maxScore, order, null, null);
  }

  @Override
  public CompletableFuture<FetchResponse> fetchByScore() {
    return leaderboardDataClient.fetchByScore(
        cacheName, leaderboardName, null, null, null, null, null);
  }

  @Override
  public CompletableFuture<FetchResponse> fetchByRank(
      int startRank, int endRank, @Nullable SortOrder order) {
    return leaderboardDataClient.fetchByRank(cacheName, leaderboardName, startRank, endRank, order);
  }

  @Override
  public CompletableFuture<FetchResponse> getRank(
      @Nonnull Iterable<Integer> ids, @Nullable SortOrder order) {
    return leaderboardDataClient.getRank(cacheName, leaderboardName, ids, order);
  }

  @Override
  public CompletableFuture<LengthResponse> length() {
    return leaderboardDataClient.length(cacheName, leaderboardName);
  }

  @Override
  public CompletableFuture<RemoveElementsResponse> removeElements(@Nonnull Iterable<Integer> ids) {
    return leaderboardDataClient.removeElements(cacheName, leaderboardName, ids);
  }

  @Override
  public CompletableFuture<DeleteResponse> delete() {
    return leaderboardDataClient.delete(cacheName, leaderboardName);
  }
}
