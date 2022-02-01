package momento.sdk.exceptions;

public class TimeoutException extends CacheServiceException {
  public TimeoutException(String message) {
    super(message);
  }
}
