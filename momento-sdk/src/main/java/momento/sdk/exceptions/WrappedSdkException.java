package momento.sdk.exceptions;

/** An SdkException that is a wrapper around another SdkException. */
public class WrappedSdkException extends SdkException {
  public WrappedSdkException(SdkException cause) {
    super(cause.getMessage(), cause, cause.getTransportErrorDetails().orElse(null));
  }
}
