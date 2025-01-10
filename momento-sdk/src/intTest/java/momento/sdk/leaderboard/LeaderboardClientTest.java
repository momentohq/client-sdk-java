package momento.sdk.leaderboard;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import momento.sdk.ILeaderboard;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.RemoveElementsResponse;
import momento.sdk.responses.leaderboard.UpsertResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class LeaderboardClientTest extends BaseLeaderboardTestClass {

  // upsert

  @Test
  public void upsertHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(3);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2, 3);
            });

    elements.clear();
    elements.put(2, 4.0);
    elements.put(3, 3.0);
    elements.put(4, 2.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(4);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 4, 3, 2);
            });
  }

  @Test
  public void upsertInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(UpsertResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void upsertNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(UpsertResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void upsertInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(UpsertResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void upsertInvalidElements() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(UpsertResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // fetch by rank

  @Test
  public void fetchByRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(3);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2, 3);
            });

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(3);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(3, 2, 1);
            });

    assertThat(leaderboard.fetchByRank(0, 2, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(2);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2);
            });

    assertThat(leaderboard.fetchByRank(1, 2, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(1);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2);
            });
  }

  @Test
  public void fetchByRankInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    assertThat(leaderboard.fetchByRank(0, 1, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void fetchByRankNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    assertThat(leaderboard.fetchByRank(0, 1, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void fetchByRankInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.fetchByRank(0, 1, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void fetchByRankInvalidRankRange() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.fetchByRank(-1, 0, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));

    assertThat(leaderboard.fetchByRank(10, 1, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // remove elements

  @Test
  public void removeElementsHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(3);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2, 3);
            });

    assertThat(leaderboard.removeElements(Collections.singleton(1)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(RemoveElementsResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(2);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2, 3);
            });

    assertThat(leaderboard.removeElements(Collections.singleton(99999)))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(RemoveElementsResponse.Success.class);

    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).hasSize(2);
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2, 3);
            });
  }

  @Test
  public void removeElementsInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    final List<Integer> ids = new ArrayList<>();
    ids.add(1);

    assertThat(leaderboard.removeElements(ids))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(RemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void removeElementsNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    final List<Integer> ids = new ArrayList<>();
    ids.add(1);

    assertThat(leaderboard.removeElements(ids))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(RemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void removeElementsInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final List<Integer> ids = new ArrayList<>();
    ids.add(1);

    assertThat(leaderboard.removeElements(ids))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(RemoveElementsResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }
}
