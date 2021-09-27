package momento.sdk.exceptions;

public class SdkException extends RuntimeException {
  public SdkException(Exception cause) {
    this.initCause(cause);
  }
}
