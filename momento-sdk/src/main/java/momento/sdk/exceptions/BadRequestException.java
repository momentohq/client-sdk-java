package momento.sdk.exceptions;

/** Represents exceptions thrown when invalid parameters are passed to the Cache Service */
public class BadRequestException extends CacheServiceException {
  public BadRequestException(String message) {
    super(message);
  }
}
