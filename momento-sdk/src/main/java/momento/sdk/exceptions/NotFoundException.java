package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested resource or the resource on which an operation was requested doesn't exist. */
@Deprecated // Use CacheNotFoundException instead
public class NotFoundException extends CacheNotFoundException {

  /**
   * Constructs a NotFoundException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public NotFoundException(Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(cause, transportErrorDetails);
  }
}
