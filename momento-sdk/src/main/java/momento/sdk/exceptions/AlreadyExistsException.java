package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** A resource already exists. */
@Deprecated // Use CacheAlreadyExistsException instead
public class AlreadyExistsException extends CacheAlreadyExistsException {

  /**
   * Constructs an AlreadyExistsException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public AlreadyExistsException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(cause, transportErrorDetails);
  }
}
