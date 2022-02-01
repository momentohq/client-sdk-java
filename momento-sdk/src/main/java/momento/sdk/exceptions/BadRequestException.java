package momento.sdk.exceptions;

/** Represents exceptions thrown when invalid parameters are sent to Momento Services. */
public class BadRequestException extends MomentoServiceException {
  public BadRequestException(String message) {
    super(message);
  }
}
