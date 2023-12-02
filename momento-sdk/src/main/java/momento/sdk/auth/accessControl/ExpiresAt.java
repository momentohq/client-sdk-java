package momento.sdk.auth.accessControl;

public class ExpiresAt extends Expiration {
  private final Integer validUntil;

  private ExpiresAt(Integer epochTimestamp) {
    super(epochTimestamp != 0 && epochTimestamp != null);
    if (doesExpire()) {
      this.validUntil = epochTimestamp;
    } else {
      this.validUntil = null;
    }
  }

  public Integer getEpoch() {
    return validUntil;
  }

  public static ExpiresAt fromEpoch(Integer epoch) {
    return new ExpiresAt(epoch);
  }
}
