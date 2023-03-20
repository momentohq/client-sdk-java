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

  /**
   * Constructs a ClientSdkException with an error code, a detail message, and a cause.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   * @param cause the cause.
   */
  public ClientSdkException(MomentoErrorCode errorCode, String message, Throwable cause) {
    super(errorCode, message, cause);
  }

  /**
   * Constructs a ClientSdkException with an error code and a detail message.
   *
   * @param errorCode the error code, or {@link MomentoErrorCode#UNKNOWN} if none exists.
   * @param message the detail message.
   */
  public ClientSdkException(MomentoErrorCode errorCode, String message) {
    super(errorCode, message);
  }

  /**
   * Constructs a ClientSdkException with a detail message.
   *
   * @param message the detail message.
   */
  public ClientSdkException(String message) {
    super(MomentoErrorCode.UNKNOWN, message);
  }
}
