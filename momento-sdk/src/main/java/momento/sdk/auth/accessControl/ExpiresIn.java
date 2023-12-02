package momento.sdk.auth.accessControl;

import java.time.Instant;

public class ExpiresIn extends Expiration {
  private final Integer validForSeconds;

  private ExpiresIn(Integer validForSeconds) {
    super(validForSeconds != null);
    this.validForSeconds = validForSeconds;
  }

  public Integer getSeconds() {
    return validForSeconds;
  }

  public static ExpiresIn never() {
    return new ExpiresIn(null);
  }

  public static ExpiresIn seconds(int validForSeconds) {
    return new ExpiresIn(validForSeconds);
  }

  public static ExpiresIn minutes(int validForMinutes) {
    return new ExpiresIn(validForMinutes * 60);
  }

  public static ExpiresIn hours(int validForHours) {
    return new ExpiresIn(validForHours * 3600);
  }

  public static ExpiresIn days(int validForDays) {
    return new ExpiresIn(validForDays * 86400);
  }

  public static ExpiresIn epoch(long expiresIn) {
    long now = Instant.now().getEpochSecond();
    return new ExpiresIn((int) (expiresIn - now));
  }

  public static ExpiresIn epoch(int expiresIn) {
    return epoch((long) expiresIn);
  }
}
