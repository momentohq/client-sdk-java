package momento.sdk.exceptions;

/**
 * Base class for all exceptions thrown by the SDK
 */
public class SdkException extends RuntimeException {

  public SdkException(String message, Throwable cause) {
    super(message, cause);
    this.initCause(cause);
  }

  public SdkException(String message) {
    super(message);
  }
}
