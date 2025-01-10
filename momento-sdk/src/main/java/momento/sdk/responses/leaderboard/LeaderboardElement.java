package momento.sdk.responses.leaderboard;

import javax.annotation.Nonnull;

/**
 * A pairing of an integer ID to a score. LeaderboardElements have equality based on their ID and
 * ordering based on their score.
 */
public class LeaderboardElement implements Comparable<LeaderboardElement> {
  private final int id;
  private final double score;

  /**
   * Constructs a LeaderboardElement with an ID and a score.
   *
   * @param id The element's ID.
   * @param score The element's score.
   */
  public LeaderboardElement(int id, double score) {
    this.id = id;
    this.score = score;
  }

  /**
   * Gets the ID.
   *
   * @return the ID
   */
  public int getId() {
    return id;
  }

  /**
   * Gets the score.
   *
   * @return the score
   */
  public double getScore() {
    return score;
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
