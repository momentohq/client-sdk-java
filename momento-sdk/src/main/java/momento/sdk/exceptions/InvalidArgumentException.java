package momento.sdk.exceptions;

/** Represents exceptions thrown when invalid parameters are passed to the Cache Service */
public class InvalidArgumentException extends CacheServiceException {
  public InvalidArgumentException(String message) {
    super(message);
  }
}
