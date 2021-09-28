package momento.sdk.exceptions;

/**
 * Represents all client side exceptions thrown by the SDK.
 *
 * <p>This exception typically implies that the request wasn't sent to the service successfully or
 * if the service responded, the sdk couldn't interpret the response. An example would be SDK client
 * was unable to convert the user provided data into a valid request that was expected by the
 * service.
 */
public class ClientSdkException extends SdkException {

  public ClientSdkException(String message, Throwable cause) {
    super(message, cause);
  }

  public ClientSdkException(String message) {
    super(message);
  }
}
