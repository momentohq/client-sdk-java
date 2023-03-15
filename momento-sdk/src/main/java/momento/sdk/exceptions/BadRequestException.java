package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Invalid parameters sent to Momento Services. */
public class BadRequestException extends MomentoServiceException {

  public BadRequestException(String message, MomentoTransportErrorDetails transportErrorDetails) {
    super(message, transportErrorDetails);
  }
}
