package momento.sdk.exceptions;

import java.util.Optional;
import momento.sdk.internal.MomentoTransportErrorDetails;

/** Base class for all exceptions thrown by the SDK */
public class SdkException extends RuntimeException {

  /** The error code. */
  private final MomentoErrorCode errorCode;

  /** Transport layer details about the error. */
  private final MomentoTransportErrorDetails transportErrorDetails;

  /**
   * Constructs an SdkException from another SdkException. The detail message, error code, and error
   * details are copied from the given exception and the cause is set to the given exception.
   *
   * @param sdkException the exception to wrap.
   */
  public SdkException(SdkException sdkException) {
    super(sdkException.getMessage(), sdkException);
    this.errorCode = sdkException.errorCode;
    this.transportErrorDetails = sdkException.transportErrorDetails;
  }

  /**
   * Constructs an SdkException with an error code, a detail message, a cause, and error details.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public SdkException(
      MomentoErrorCode errorCode,
      String message,
      Throwable cause,
      MomentoTransportErrorDetails transportErrorDetails) {
    super(message, cause);
    this.errorCode = errorCode;
    this.transportErrorDetails = transportErrorDetails;
  }

  /**
   * Constructs an SdkException with an error code, a detail message, and a cause.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param cause the cause.
   */
  public SdkException(MomentoErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
    this.transportErrorDetails = null;
  }

  /**
   * Constructs an SdkException with an error code, a detail message, and error details.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param transportErrorDetails details about the request and error.
   */
  public SdkException(
      MomentoErrorCode errorCode,
      String message,
      MomentoTransportErrorDetails transportErrorDetails) {
    super(message);
    this.errorCode = errorCode;
    this.transportErrorDetails = transportErrorDetails;
  }

  /**
   * Constructs an SdkException with an error code and a detail message.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   */
  public SdkException(MomentoErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
    this.transportErrorDetails = null;
  }

  /**
   * Constructs an SdkException with a detail message and an {@link MomentoErrorCode#UNKNOWN} error
   * code.
   *
   * @param message the detail message.
   */
  public SdkException(String message) {
    super(message);
    this.errorCode = MomentoErrorCode.UNKNOWN;
    this.transportErrorDetails = null;
  }

  /**
   * Returns the Momento error code.
   *
   * @return the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   */
  public MomentoErrorCode getErrorCode() {
    return errorCode;
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

  protected String buildToString(String className) {
    StringBuilder sb = new StringBuilder();
    sb.append(className).append("{");
    sb.append("message=\"").append(getMessage()).append("\"");
    sb.append(", errorCode=").append(errorCode);
    if (transportErrorDetails != null) {
      sb.append(", transportErrorDetails=").append(transportErrorDetails);
    } else {
      sb.append(", transportErrorDetails=null");
    }
    sb.append("}");
    return sb.toString();
  }
}
