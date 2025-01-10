package momento.sdk;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.FetchResponse;
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
   * Fetch the elements in the given leaderboard by index (rank). Note: can fetch a maximum of 8192
   * elements at a time and rank is 0-based (index begins at 0).
   *
   * @param startRank The rank of the first element to fetch. This rank is inclusive, i.e. the
   *     element at this rank will be fetched. Ranks can be used to manually paginate through the
   *     leaderboard in batches of 8192 elements (e.g. request 0-8192, then 8192-16384, etc.).
   * @param endRank The rank of the last element to fetch. This rank is exclusive, i.e. the element
   *     at this rank will not be fetched. Ranks can be used to manually paginate through the
   *     leaderboard in batches of 8192 elements (e.g. request 0-8192, then 8192-16384, etc.).
   * @param order The order to fetch the elements in.
   * @return A future containing the result of the fetch operation: {@link FetchResponse.Success}
   *     containing the elements, or {@link FetchResponse.Error}.
   */
  CompletableFuture<FetchResponse> fetchByRank(
      int startRank, int endRank, @Nonnull SortOrder order);

  /**
   * Remove multiple elements from the given leaderboard Note: can remove a maximum of 8192 elements
   * at a time.
   *
   * @param ids The IDs of the elements to remove from the leaderboard.
   * @return A future containing the result of the remove operation: {@link
   *     RemoveElementsResponse.Success} or {@link RemoveElementsResponse.Error}.
   */
  CompletableFuture<RemoveElementsResponse> removeElements(@Nonnull Iterable<Integer> ids);
}
