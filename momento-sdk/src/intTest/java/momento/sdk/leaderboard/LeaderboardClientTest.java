package momento.sdk.leaderboard;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import momento.sdk.ILeaderboard;
import momento.sdk.exceptions.CacheNotFoundException;
import momento.sdk.exceptions.InvalidArgumentException;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.DeleteResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.LengthResponse;
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
    //noinspection DataFlowIssue
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

  // fetch by score

  @Test
  public void fetchByScoreHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    // ascending
    assertThat(leaderboard.fetchByScore())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2, 3);
            });
    assertThat(leaderboard.fetchByScore(null, null, null, 1, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2, 3);
            });
    assertThat(leaderboard.fetchByScore(null, null, SortOrder.ASCENDING, 0, 2))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2);
            });

    // descending
    assertThat(leaderboard.fetchByScore(null, null, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(3, 2, 1);
            });
    assertThat(leaderboard.fetchByScore(null, null, SortOrder.DESCENDING, 1, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2, 1);
            });
    assertThat(leaderboard.fetchByScore(null, null, SortOrder.DESCENDING, 0, 2))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(3, 2);
            });

    // limited by max score
    assertThat(leaderboard.fetchByScore(null, 2.1, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2);
            });

    // limited by min score
    assertThat(leaderboard.fetchByScore(1.1, null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2, 3);
            });

    // limited by min score and max score
    assertThat(leaderboard.fetchByScore(1.1, 3.0, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2);
            });
  }

  @Test
  public void fetchByScoreInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    //noinspection DataFlowIssue
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    assertThat(leaderboard.fetchByScore(null, null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void fetchByScoreNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    assertThat(leaderboard.fetchByScore(null, null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void fetchByScoreInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.fetchByScore(null, null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void fetchByScoreInvalidScoreRange() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.fetchByScore(10.0, 1.0, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
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

    // ascending
    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2, 3);
            });

    // descending
    assertThat(leaderboard.fetchByRank(0, 10, SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(3, 2, 1);
            });

    // rank range smaller than leaderboard size
    assertThat(leaderboard.fetchByRank(0, 2, null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(1, 2);
            });

    assertThat(leaderboard.fetchByRank(1, 2, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp -> {
              final List<LeaderboardElement> scoredElements = resp.elementsList();
              assertThat(scoredElements).map(LeaderboardElement::getId).containsExactly(2);
            });
  }

  @Test
  public void fetchByRankInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    //noinspection DataFlowIssue
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

  // get rank

  @Test
  public void getRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    // ascending
    assertThat(leaderboard.getRank(elements.keySet(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 0), tuple(2, 1), tuple(3, 2)));

    // descending
    assertThat(leaderboard.getRank(elements.keySet(), SortOrder.DESCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 2), tuple(2, 1), tuple(3, 0)));

    // ids are a subset of the leaderboard
    assertThat(leaderboard.getRank(new HashSet<>(Arrays.asList(1, 2)), null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 0), tuple(2, 1)));

    // ids are a superset of the leaderboard
    assertThat(leaderboard.getRank(new HashSet<>(Arrays.asList(1, 2, 3, 4)), null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 0), tuple(2, 1), tuple(3, 2)));
  }

  @Test
  public void getRankInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    //noinspection DataFlowIssue
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    assertThat(leaderboard.getRank(Collections.singleton(1), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void getRankNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    assertThat(leaderboard.getRank(Collections.singleton(1), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void getRankInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.getRank(Collections.singleton(1), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void getRankInvalidIds() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    //noinspection DataFlowIssue
    assertThat(leaderboard.getRank(null, SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  // length

  @Test
  public void lengthHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Success.class))
        .satisfies(resp -> assertThat(resp.length()).isEqualTo(0));

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Success.class))
        .satisfies(resp -> assertThat(resp.length()).isEqualTo(3));
  }

  @Test
  public void lengthInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    //noinspection DataFlowIssue
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void lengthNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void lengthInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Error.class))
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
    //noinspection DataFlowIssue
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

  // delete

  @Test
  public void deleteHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(1, 1.0);
    elements.put(2, 2.0);
    elements.put(3, 3.0);

    assertThat(leaderboard.delete())
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DeleteResponse.Success.class);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Success.class))
        .satisfies(resp -> assertThat(resp.length()).isEqualTo(3));

    assertThat(leaderboard.delete())
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(DeleteResponse.Success.class);

    assertThat(leaderboard.length())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(LengthResponse.Success.class))
        .satisfies(resp -> assertThat(resp.length()).isEqualTo(0));
  }

  @Test
  public void deleteInvalidCacheName() {
    final String leaderboardName = randomString("leaderboard");
    //noinspection DataFlowIssue
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(null, leaderboardName);

    assertThat(leaderboard.delete())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void deleteNonExistentCache() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(randomString(), leaderboardName);

    assertThat(leaderboard.delete())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(CacheNotFoundException.class));
  }

  @Test
  public void deleteInvalidLeaderboardName() {
    final String leaderboardName = "";
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    assertThat(leaderboard.delete())
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(DeleteResponse.Error.class))
        .satisfies(error -> assertThat(error).hasCauseInstanceOf(InvalidArgumentException.class));
  }

  @Test
  public void getCompetitionRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard = leaderboardClient.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(0, 20.0);
    elements.put(1, 10.0);
    elements.put(2, 10.0);
    elements.put(3, 5.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    // descending
    assertThat(leaderboard.getCompetitionRank(elements.keySet(), null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(0, 0), tuple(1, 1), tuple(2, 1), tuple(3, 3)));

    // ascending
    assertThat(leaderboard.getCompetitionRank(elements.keySet(), SortOrder.ASCENDING))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(0, 3), tuple(1, 1), tuple(2, 1), tuple(3, 0)));

    // ids are a subset of the leaderboard
    assertThat(leaderboard.getCompetitionRank(new HashSet<>(Arrays.asList(1, 2)), null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 1), tuple(2, 1)));

    // ids are a superset of the leaderboard
    assertThat(leaderboard.getCompetitionRank(new HashSet<>(Arrays.asList(1, 2, 3, 4)), null))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 1), tuple(2, 1), tuple(3, 3)));
  }
}
