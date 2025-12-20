package momento.sdk.leaderboard;

import static momento.sdk.TestUtils.randomString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import momento.sdk.ILeaderboard;
import momento.sdk.responses.SortOrder;
import momento.sdk.responses.leaderboard.DeleteResponse;
import momento.sdk.responses.leaderboard.FetchResponse;
import momento.sdk.responses.leaderboard.LeaderboardElement;
import momento.sdk.responses.leaderboard.LengthResponse;
import momento.sdk.responses.leaderboard.RemoveElementsResponse;
import momento.sdk.responses.leaderboard.UpsertResponse;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

public class LeaderboardClientTestApiKeyV2 extends BaseLeaderboardTestClass {

  @Test
  public void upsertHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void fetchByScoreHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void fetchByRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void getRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void lengthHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void removeElementsHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void deleteHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

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
  public void getCompetitionRankHappyPath() {
    final String leaderboardName = randomString("leaderboard");
    final ILeaderboard leaderboard =
        leaderboardClientApiKeyV2.leaderboard(cacheName, leaderboardName);

    final Map<Integer, Double> elements = new HashMap<>();
    elements.put(0, 20.0);
    elements.put(1, 10.0);
    elements.put(2, 10.0);
    elements.put(3, 5.0);

    assertThat(leaderboard.upsert(elements))
        .succeedsWithin(FIVE_SECONDS)
        .isInstanceOf(UpsertResponse.Success.class);

    // descending
    assertThat(leaderboard.getCompetitionRank(elements.keySet()))
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
    assertThat(leaderboard.getCompetitionRank(new HashSet<>(Arrays.asList(1, 2))))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 1), tuple(2, 1)));

    // ids are a superset of the leaderboard
    assertThat(leaderboard.getCompetitionRank(new HashSet<>(Arrays.asList(1, 2, 3, 4))))
        .succeedsWithin(FIVE_SECONDS)
        .asInstanceOf(InstanceOfAssertFactories.type(FetchResponse.Success.class))
        .satisfies(
            resp ->
                assertThat(resp.elementsList())
                    .extracting("id", "rank")
                    .containsExactly(tuple(1, 1), tuple(2, 1), tuple(3, 3)));
  }
}
