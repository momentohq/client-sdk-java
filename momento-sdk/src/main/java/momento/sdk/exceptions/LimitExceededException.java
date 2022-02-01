package momento.sdk.exceptions;

public class LimitExceededException extends CacheServiceException {
  public LimitExceededException(String message) {
    super(message);
  }
}
