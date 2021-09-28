package momento.sdk.exceptions;

/** Base class for all exceptions resulting from Cache Service interactions. */
public class CacheServiceException extends MomentoServiceException {

  public CacheServiceException(String message) {
    super(message);
  }
}
