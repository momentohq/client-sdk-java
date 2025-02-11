package momento.sdk;

import static momento.sdk.ValidationUtils.checkCacheNameValid;
import static momento.sdk.ValidationUtils.checkScoreRangeValid;
import static momento.sdk.ValidationUtils.validateCount;
import static momento.sdk.ValidationUtils.validateLeaderboardElements;
import static momento.sdk.ValidationUtils.validateLeaderboardName;
import static momento.sdk.ValidationUtils.validateNotNull;
import static momento.sdk.ValidationUtils.validateOffset;
import static momento.sdk.ValidationUtils.validateRankRange;

import com.google.common.util.concurrent.ListenableFuture;
import grpc.common._Empty;
import grpc.leaderboard._DeleteLeaderboardRequest;
import grpc.leaderboard._Element;
import grpc.leaderboard._GetByRankRequest;
import grpc.leaderboard._GetByRankResponse;
import grpc.leaderboard._GetByScoreRequest;
import grpc.leaderboard._GetByScoreResponse;
import grpc.leaderboard._GetCompetitionRankRequest;
import grpc.leaderboard._GetCompetitionRankResponse;
import grpc.leaderboard._GetLeaderboardLengthRequest;
import grpc.leaderboard._GetLeaderboardLengthResponse;
import grpc.leaderboard._GetRankRequest;
import grpc.leaderboard._GetRankResponse;
import grpc.leaderboard._Order;
import grpc.leaderboard._RankRange;
import grpc.leaderboard._RankedElement;
import grpc.leaderboard._RemoveElementsRequest;
import grpc.leaderboard._ScoreRange;
import grpc.leaderboard._UpsertElementsRequest;
import io.grpc.Metadata;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfiguration;
import momento.sdk.exceptions.CacheServiceExceptionMapper;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.DeleteResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.LengthResponse;
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

  public CompletableFuture<FetchResponse> fetchByScore(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      checkScoreRangeValid(minScore, maxScore);
      validateOffset(offset);
      validateCount(count);

      return sendFetchByScore(cacheName, leaderboardName, minScore, maxScore, order, offset, count);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new FetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<FetchResponse> fetchByRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      int startRank,
      int endRank,
      @Nullable SortOrder order) {
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

  public CompletableFuture<FetchResponse> getRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Iterable<Integer> ids,
      @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      validateNotNull(ids, "ids");

      return sendGetRank(cacheName, leaderboardName, ids, order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new FetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<LengthResponse> length(
      @Nonnull String cacheName, @Nonnull String leaderboardName) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);

      return sendLength(cacheName, leaderboardName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new LengthResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<RemoveElementsResponse> removeElements(
      @Nonnull String cacheName, @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      validateNotNull(ids, "ids");

      return sendRemoveElements(cacheName, leaderboardName, ids);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new RemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<DeleteResponse> delete(
      @Nonnull String cacheName, @Nonnull String leaderboardName) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);

      return sendDelete(cacheName, leaderboardName);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e)));
    }
  }

  public CompletableFuture<FetchResponse> getCompetitionRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Iterable<Integer> ids,
      @Nullable SortOrder order) {
    try {
      checkCacheNameValid(cacheName);
      validateLeaderboardName(leaderboardName);
      validateNotNull(ids, "ids");

      return sendgetCompetitionRank(cacheName, leaderboardName, ids, order);
    } catch (Exception e) {
      return CompletableFuture.completedFuture(
          new FetchResponse.Error(CacheServiceExceptionMapper.convert(e)));
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
                .upsertElements(buildUpsertElementsRequest(leaderboardName, elements));

    final Function<_Empty, UpsertResponse> success = rsp -> new UpsertResponse.Success();

    final Function<Throwable, UpsertResponse> failure =
        e -> new UpsertResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  CompletableFuture<FetchResponse> sendFetchByScore(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetByScoreResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getByScore(
                    buildGetByScoreRequest(
                        leaderboardName, minScore, maxScore, order, offset, count));

    final Function<_GetByScoreResponse, FetchResponse> success =
        rsp -> new FetchResponse.Success(convertToLeaderboardElements(rsp.getElementsList()));

    final Function<Throwable, FetchResponse> failure =
        e -> new FetchResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  CompletableFuture<FetchResponse> sendFetchByRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      int startRank,
      int endRank,
      @Nullable SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetByRankResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getByRank(buildGetByRankRequest(leaderboardName, startRank, endRank, order));

    final Function<_GetByRankResponse, FetchResponse> success =
        rsp -> new FetchResponse.Success(convertToLeaderboardElements(rsp.getElementsList()));

    final Function<Throwable, FetchResponse> failure =
        e -> new FetchResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  CompletableFuture<FetchResponse> sendGetRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Iterable<Integer> ids,
      @Nullable SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetRankResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getRank(buildGetRankRequest(leaderboardName, ids, order));

    final Function<_GetRankResponse, FetchResponse> success =
        rsp -> new FetchResponse.Success(convertToLeaderboardElements(rsp.getElementsList()));

    final Function<Throwable, FetchResponse> failure =
        e -> new FetchResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  CompletableFuture<FetchResponse> sendgetCompetitionRank(
      @Nonnull String cacheName,
      @Nonnull String leaderboardName,
      @Nonnull Iterable<Integer> ids,
      @Nullable SortOrder order) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetCompetitionRankResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getCompetitionRank(buildgetCompetitionRankRequest(leaderboardName, ids, order));

    final Function<_GetCompetitionRankResponse, FetchResponse> success =
        rsp -> new FetchResponse.Success(convertToLeaderboardElements(rsp.getElementsList()));

    final Function<Throwable, FetchResponse> failure =
        e -> new FetchResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private List<LeaderboardElement> convertToLeaderboardElements(List<_RankedElement> elements) {
    return elements.stream()
        .map(e -> new LeaderboardElement(e.getId(), e.getScore(), e.getRank()))
        .collect(Collectors.toList());
  }

  private CompletableFuture<LengthResponse> sendLength(
      @Nonnull String cacheName, @Nonnull String leaderboardName) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_GetLeaderboardLengthResponse>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .getLeaderboardLength(buildLengthRequest(leaderboardName));

    final Function<_GetLeaderboardLengthResponse, LengthResponse> success =
        rsp -> {
          final int length = rsp.getCount();
          return new LengthResponse.Success(length);
        };

    final Function<Throwable, LengthResponse> failure =
        e -> new LengthResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<RemoveElementsResponse> sendRemoveElements(
      @Nonnull String cacheName, @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_Empty>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .removeElements(buildRemoveElementsRequest(leaderboardName, ids));

    final Function<_Empty, RemoveElementsResponse> success =
        rsp -> new RemoveElementsResponse.Success();

    final Function<Throwable, RemoveElementsResponse> failure =
        e -> new RemoveElementsResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private CompletableFuture<DeleteResponse> sendDelete(
      @Nonnull String cacheName, @Nonnull String leaderboardName) {
    final Metadata metadata = metadataWithCache(cacheName);
    final Supplier<ListenableFuture<_Empty>> stubSupplier =
        () ->
            attachMetadata(stubsManager.getStub(), metadata)
                .deleteLeaderboard(buildDeleteRequest(leaderboardName));

    final Function<_Empty, DeleteResponse> success = rsp -> new DeleteResponse.Success();

    final Function<Throwable, DeleteResponse> failure =
        e -> new DeleteResponse.Error(CacheServiceExceptionMapper.convert(e));

    return executeGrpcFunction(stubSupplier, success, failure);
  }

  private _UpsertElementsRequest buildUpsertElementsRequest(
      @Nonnull String leaderboardName, @Nonnull Map<Integer, Double> elements) {
    return _UpsertElementsRequest.newBuilder()
        .setLeaderboard(leaderboardName)
        .addAllElements(
            elements.entrySet().stream()
                .map(e -> _Element.newBuilder().setId(e.getKey()).setScore(e.getValue()).build())
                .collect(Collectors.toList()))
        .build();
  }

  private _GetByScoreRequest buildGetByScoreRequest(
      @Nonnull String leaderboardName,
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count) {
    final _ScoreRange.Builder scoreBuilder = _ScoreRange.newBuilder();
    if (minScore != null) {
      scoreBuilder.setMinInclusive(minScore);
    } else {
      scoreBuilder.setUnboundedMin(scoreBuilder.getUnboundedMin());
    }
    if (maxScore != null) {
      scoreBuilder.setMaxExclusive(maxScore);
    } else {
      scoreBuilder.setUnboundedMax(scoreBuilder.getUnboundedMax());
    }

    final _GetByScoreRequest.Builder requestBuilder =
        _GetByScoreRequest.newBuilder()
            .setLeaderboard(leaderboardName)
            .setScoreRange(scoreBuilder.build());

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_Order.ASCENDING);
    }

    if (offset != null) {
      requestBuilder.setOffset(offset);
    } else {
      requestBuilder.setOffset(0);
    }

    if (count != null) {
      requestBuilder.setLimitElements(count);
    } else {
      requestBuilder.setLimitElements(8192);
    }

    return requestBuilder.build();
  }

  private _GetByRankRequest buildGetByRankRequest(
      @Nonnull String leaderboardName, int startRank, int endRank, @Nullable SortOrder order) {
    final _GetByRankRequest.Builder requestBuilder =
        _GetByRankRequest.newBuilder()
            .setLeaderboard(leaderboardName)
            .setRankRange(
                _RankRange.newBuilder()
                    .setStartInclusive(startRank)
                    .setEndExclusive(endRank)
                    .build());

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_Order.ASCENDING);
    }
    return requestBuilder.build();
  }

  private _GetRankRequest buildGetRankRequest(
      @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids, @Nullable SortOrder order) {
    final _GetRankRequest.Builder requestBuilder =
        _GetRankRequest.newBuilder().setLeaderboard(leaderboardName).addAllIds(ids);

    if (order == SortOrder.DESCENDING) {
      requestBuilder.setOrder(_Order.DESCENDING);
    } else {
      requestBuilder.setOrder(_Order.ASCENDING);
    }

    return requestBuilder.build();
  }

  private _GetLeaderboardLengthRequest buildLengthRequest(@Nonnull String leaderboardName) {
    return _GetLeaderboardLengthRequest.newBuilder().setLeaderboard(leaderboardName).build();
  }

  private _RemoveElementsRequest buildRemoveElementsRequest(
      @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids) {
    return _RemoveElementsRequest.newBuilder()
        .setLeaderboard(leaderboardName)
        .addAllIds(ids)
        .build();
  }

  private _DeleteLeaderboardRequest buildDeleteRequest(@Nonnull String leaderboardName) {
    return _DeleteLeaderboardRequest.newBuilder().setLeaderboard(leaderboardName).build();
  }

  private _GetCompetitionRankRequest buildgetCompetitionRankRequest(
      @Nonnull String leaderboardName, @Nonnull Iterable<Integer> ids, @Nullable SortOrder order) {
    final _GetCompetitionRankRequest.Builder requestBuilder =
        _GetCompetitionRankRequest.newBuilder().setLeaderboard(leaderboardName).addAllIds(ids);

    if (order != null) {
      if (order == SortOrder.DESCENDING) {
        requestBuilder.setOrder(_Order.DESCENDING);
      } else {
        requestBuilder.setOrder(_Order.ASCENDING);
      }
    } else {
      requestBuilder.setOrder(_Order.DESCENDING);
    }

    return requestBuilder.build();
  }

  @Override
  public void doClose() {
    stubsManager.close();
  }
}
