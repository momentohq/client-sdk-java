package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** The server temporarily could not be reached. */
public class ServerUnavailableException extends MomentoServiceException {

  private static final String MESSAGE =
      "The server was unable to handle the request; consider retrying. If the error persists, please contact Momento.";

  /**
   * Constructs a ServerUnavailableException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   */
  public ServerUnavailableException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails) {
    super(MomentoErrorCode.SERVER_UNAVAILABLE, MESSAGE, cause, transportErrorDetails);
  }
}
