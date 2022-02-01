package momento.sdk.exceptions;

/** Exception when operation was cancelled. */
public class CancellationException extends CacheServiceException {
  public CancellationException(String message) {
    super(message);
  }
}
