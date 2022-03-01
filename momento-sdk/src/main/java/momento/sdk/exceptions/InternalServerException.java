package momento.sdk.exceptions;

/** Momento Service encountered an unexpected exception while trying to fulfill the request. */
public class InternalServerException extends MomentoServiceException {

  public InternalServerException(String message) {
    super(message);
  }

  public InternalServerException(String message, Throwable cause) {
    super(message, cause);
  }
}
