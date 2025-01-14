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

public interface ILeaderboard {

  /**
   * Updates elements in a leaderboard or inserts elements if they do not already exist. The
   * leaderboard is also created if it does not already exist. Note: can upsert a maximum of 8192
   * elements at a time.
   *
   * @param elements The ID->score pairs to add to the leaderboard.
   * @return A future containing the result of the upsert operation: {@link UpsertResponse.Success}
   *     or {@link UpsertResponse.Error}.
   */
  CompletableFuture<UpsertResponse> upsert(@Nonnull Map<Integer, Double> elements);

  /**
   * Fetch the elements of the leaderboard by score. Note: can fetch a maximum of 8192 elements at a
   * time.
   *
   * @param minScore The minimum score (inclusive) of the elements to fetch. Defaults to negative
   *     infinity.
   * @param maxScore The maximum score (exclusive) of the elements to fetch. Defaults to positive
   *     infinity.
   * @param order The order to fetch the elements in. Defaults to {@link SortOrder#ASCENDING}.
   * @param offset The number of elements to skip before returning the first element. Defaults to 0.
   *     Note: this is not the score of the first element to return, but the number of elements of
   *     the result set to skip before returning the first element.
   * @param count The maximum number of elements to return. Defaults to 8192, which is the maximum
   *     that can be fetched at a time.
   * @return A future containing the result of the fetch operation: {@link FetchResponse.Success}
   *     containing the elements, or {@link FetchResponse.Error}.
   */
  CompletableFuture<FetchResponse> fetchByScore(
      @Nullable Double minScore,
      @Nullable Double maxScore,
      @Nullable SortOrder order,
      @Nullable Integer offset,
      @Nullable Integer count);

  /**
   * Fetch the elements of the leaderboard by score. Note: can fetch a maximum of 8192 elements at a
   * time.
   *
   * @param minScore The minimum score (inclusive) of the elements to fetch. Defaults to negative
   *     infinity.
   * @param maxScore The maximum score (exclusive) of the elements to fetch. Defaults to positive
   *     infinity.
   * @param order The order to fetch the elements in. Defaults to {@link SortOrder#ASCENDING}.
   * @return A future containing the result of the fetch operation: {@link FetchResponse.Success}
   *     containing the elements, or {@link FetchResponse.Error}.
   */
  CompletableFuture<FetchResponse> fetchByScore(
      @Nullable Double minScore, @Nullable Double maxScore, @Nullable SortOrder order);

  /**
   * Fetch the elements of the leaderboard by index (rank). Note: can fetch a maximum of 8192
   * elements at a time and rank is 0-based (index begins at 0).
   *
   * @param startRank The rank of the first element to fetch. This rank is inclusive, i.e. the
   *     element at this rank will be fetched. Ranks can be used to manually paginate through the
   *     leaderboard in batches of 8192 elements (e.g. request 0-8192, then 8192-16384, etc.).
   * @param endRank The rank of the last element to fetch. This rank is exclusive, i.e. the element
   *     at this rank will not be fetched. Ranks can be used to manually paginate through the
   *     leaderboard in batches of 8192 elements (e.g. request 0-8192, then 8192-16384, etc.).
   * @param order The order to fetch the elements in. Defaults to {@link SortOrder#ASCENDING}.
   * @return A future containing the result of the fetch operation: {@link FetchResponse.Success}
   *     containing the elements, or {@link FetchResponse.Error}.
   */
  CompletableFuture<FetchResponse> fetchByRank(
      int startRank, int endRank, @Nullable SortOrder order);

  /**
   * Looks up the rank of the given elements in the leaderboard. The returned elements will be
   * sorted numerically by their IDs. Note: rank is 0-based (index begins at 0).
   *
   * @param ids The IDs of the elements to fetch from the leaderboard.
   * @param order The order to fetch the elements in. Defaults to {@link SortOrder#ASCENDING}.
   * @return A future containing the result of the fetch operation: {@link FetchResponse.Success}
   *     containing the elements, or {@link FetchResponse.Error}.
   */
  CompletableFuture<FetchResponse> getRank(
      @Nonnull Iterable<Integer> ids, @Nullable SortOrder order);

  /**
   * Fetches the length (number of items) of the leaderboard.
   *
   * @return A future containing the result of the length operation: {@link LengthResponse.Success}
   *     containing the length of the leaderboard, or {@link LengthResponse.Error}.
   */
  CompletableFuture<LengthResponse> length();

  /**
   * Remove multiple elements from the leaderboard. Note: can remove a maximum of 8192 elements at a
   * time.
   *
   * @param ids The IDs of the elements to remove from the leaderboard.
   * @return A future containing the result of the remove operation: {@link
   *     RemoveElementsResponse.Success} or {@link RemoveElementsResponse.Error}.
   */
  CompletableFuture<RemoveElementsResponse> removeElements(@Nonnull Iterable<Integer> ids);

  /**
   * Deletes all elements in the leaderboard.
   *
   * @return A future containing the result of the delete operation: {@link DeleteResponse.Success},
   *     or {@link DeleteResponse.Error}.
   */
  CompletableFuture<DeleteResponse> delete();
}
