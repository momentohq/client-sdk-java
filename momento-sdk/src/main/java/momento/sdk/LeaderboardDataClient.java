package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.validateLeaderboardElements;
import static momento.sdk.ValidationUtils.validateLeaderboardName;
import static momento.sdk.ValidationUtils.validateRankRange;

import com.google.common.util.concurrent.ListenableFuture;
import grpc.common._Empty;
import grpc.leaderboard._Element;
import grpc.leaderboard._GetByRankRequest;
import grpc.leaderboard._GetByRankResponse;
import grpc.leaderboard._Order;
import grpc.leaderboard._RankRange;
import grpc.leaderboard._RemoveElementsRequest;
import grpc.leaderboard._UpsertElementsRequest;
import io.grpc.Metadata;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfiguration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.RemoveElementsResponse;
import momento.sdk.responses.leaderboard.UpsertResponse;

final class LeaderboardDataClient extends ScsClientBase {

  private final LeaderboardGrpcStubsManager stubsManager;

  LeaderboardDataClient(
      @Nonnull CredentialProvider credentialProvider,
      @Nonnull LeaderboardConfiguration configuration) {
    super(null);

    this.stubsManager = new LeaderboardGrpcStubsManager(credentialProvider, configuration);
  }

  public CompletableFuture<UpsertResponse> upsert(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Map<Integer, Double> elements) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      validateLeaderboardElements(elements);

      return sendUpsert(cacheName, leaderboardName, elements);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new UpsertResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<FetchResponse> fetchByRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      int startRank,
      int endRank,
      @Nonnull SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      validateRankRange(startRank, endRank);

      return sendFetchByRank(cacheName, leaderboardName, startRank, endRank, order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new FetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<RemoveElementsResponse> removeElements(
      @Nonnull String cacheName, @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);

      return sendRemoveElements(cacheName, leaderboardName, ids);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new RemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  private CompletableFuture<UpsertResponse> sendUpsert(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Map<Integer, Double> elements) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_Empty>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .upsertElements(buildUpsertElementsRequest(cacheName, leaderboardName, elements));

    final Function<_Empty, UpsertResponse> success = rsp -> new UpsertResponse.Success();

    final Function<Throwable, UpsertResponse> failure =
        e -> new UpsertResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  CompletableFuture<FetchResponse> sendFetchByRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      int startRank,
      int endRank,
      @Nonnull SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetByRankResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getByRank(
                    buildGetByRankRequest(cacheName, leaderboardName, startRank, endRank, order));

    final Function<_GetByRankResponse, FetchResponse> success =
        rsp -> {
          final List<LeaderboardElement> elements =
              rsp.getElementsList().stream()
                  .map(e -> new LeaderboardElement(e.getId(), e.getScore(), e.getRank()))
                  .collect(Collectors.toList());
          return new FetchResponse.Success(elements);
        };

    final Function<Throwable, FetchResponse> failure =
        e -> new FetchResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<RemoveElementsResponse> sendRemoveElements(
      @Nonnull String cacheName, @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_Empty>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .removeElements(buildRemoveElementsRequest(cacheName, leaderboardName, ids));

    final Function<_Empty, RemoveElementsResponse> success =
        rsp -> new RemoveElementsResponse.Success();

    final Function<Throwable, RemoveElementsResponse> failure =
        e -> new RemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private _UpsertElementsRequest buildUpsertElementsRequest(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Map<Integer, Double> elements) {
    return _UpsertElementsRequest.newBuilder()
        .setCacheName(cacheName)
        .setLeaderboard(leaderboardName)
        .addAllElements(
            elements.entrySet().stream()
                .map(e -> _Element.newBuilder().setId(e.getKey()).setScore(e.getValue()).build())
                .collect(Collectors.toList()))
        .build();
  }

  private _GetByRankRequest buildGetByRankRequest(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      int startRank,
      int endRank,
      @Nonnull SortOrder order) {
    return _GetByRankRequest.newBuilder()
        .setCacheName(cacheName)
        .setLeaderboard(leaderboardName)
        .setRankRange(
            _RankRange.newBuilder().setStartInclusive(startRank).setEndExclusive(endRank).build())
        .setOrder(order == SortOrder.ASCENDING ? _Order.ASCENDING : _Order.DESCENDING)
        .build();
  }

  private _RemoveElementsRequest buildRemoveElementsRequest(
      @Nonnull String cacheName, @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    return _RemoveElementsRequest.newBuilder()
        .setCacheName(cacheName)
        .setLeaderboard(leaderboardName)
        .addAllIds(ids)
        .build();
  }

  @Override
  public void doClose() {
    stubsManager.close();
  }
}
