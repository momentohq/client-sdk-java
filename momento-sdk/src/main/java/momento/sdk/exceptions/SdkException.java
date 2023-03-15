package momento.sdk.exceptions;

import java.util.Optional;
import momento.sdk.internal.MomentoTransportErrorDetails;

/** Base class for all exceptions thrown by the SDK */
public class SdkException extends RuntimeException {

  private final MomentoTransportErrorDetails transportErrorDetails;

  public SdkException(
      String message, Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, cause);
    this.transportErrorDetails = transportErrorDetails;
  }

  public SdkException(String message, Throwable cause) {
    super(message, cause);
    this.transportErrorDetails = null;
  }

  public SdkException(String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message);
    this.transportErrorDetails = transportErrorDetails;
  }

  public SdkException(String message) {
    super(message);
    this.transportErrorDetails = null;
  }

  /**
   * Gets the optional internal error details. Contains information for debugging low-level
   * transport layer issues.
   *
   * @return the error details if they exist.
   */
  public Optional<MomentoTransportErrorDetails> getTransportErrorDetails() {
    return Optional.ofNullable(transportErrorDetails);
  }
}
