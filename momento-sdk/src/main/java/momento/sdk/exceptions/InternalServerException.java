package momento.sdk.exceptions;

/** Cache Service encountered an unexpected exception while trying to fulfill the request. */
public class InternalServerException extends CacheServiceException {

  public InternalServerException(String message) {
    super(message);
  }
}
