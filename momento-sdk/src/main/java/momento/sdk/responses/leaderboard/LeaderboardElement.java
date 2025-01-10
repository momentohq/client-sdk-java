package momento.sdk.responses.leaderboard;

import javax.annotation.Nonnull;

/**
 * A leaderboard element consisting of an element ID, a score and a rank. LeaderboardElements have
 * equality based on their ID and ordering based on their score.
 */
public class LeaderboardElement implements Comparable<LeaderboardElement> {
  private final int id;
  private final double score;
  private final int rank;

  /**
   * Constructs a LeaderboardElement with an ID and a score.
   *
   * @param id The element's ID.
   * @param score The element's score.
   */
  public LeaderboardElement(int id, double score, int rank) {
    this.id = id;
    this.score = score;
    this.rank = rank;
  }

  /**
   * Gets the ID of the element.
   *
   * @return the ID
   */
  public int getId() {
    return id;
  }

  /**
   * Gets the score of the element.
   *
   * @return the score
   */
  public double getScore() {
    return score;
  }

  /**
   * Gets the rank of the element.
   *
   * @return the rank
   */
  public int getRank() {
    return rank;
  }

  @Override
  public int compareTo(@Nonnull LeaderboardElement that) {
    if (this == that) {
      return 0;
    }
    return Double.compare(this.score, that.score);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LeaderboardElement that = (LeaderboardElement) o;
    return this.id == that.id;
  }

  @Override
  public int hashCode() {
    return id;
  }
}
