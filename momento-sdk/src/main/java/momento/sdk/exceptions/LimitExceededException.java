package momento.sdk.exceptions;

import momento.sdk.internal.MomentoTransportErrorDetails;

/** Requested operation couldn't be completed because system limits were hit. */
public class LimitExceededException extends MomentoServiceException {
  /**
   * Constructs a LimitExceededException with a cause and error details.
   *
   * @param cause the cause.
   * @param transportErrorDetails details about the request and error.
   * @param messageWrapper details about which limit was exceeded.
   */
  public LimitExceededException(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails, String messageWrapper) {
    super(MomentoErrorCode.LIMIT_EXCEEDED_ERROR, messageWrapper, cause, transportErrorDetails);
  }

  /**
   * Creates a LimitExceededException with a cause and error details. Determines the limit exceeded
   * message wrapper to use based on the error cause.
   *
   * @param cause
   * @param transportErrorDetails
   * @param errorCause
   * @return
   */
  public static LimitExceededException CreateWithMessageWrapper(
      Throwable cause, MomentoTransportErrorDetails transportErrorDetails, String errorCause) {
    String messageWrapper = LimitExceededMessageWrapper.UNKNOWN_LIMIT_EXCEEDED.toString();
    switch (errorCause) {
      case "topic_subscriptions_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED.toString();
        break;
      case "operations_rate_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.OPERATIONS_RATE_LIMIT_EXCEEDED.toString();
        break;
      case "throughput_rate_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.THROUGHPUT_RATE_LIMIT_EXCEEDED.toString();
        break;
      case "request_size_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.REQUEST_SIZE_LIMIT_EXCEEDED.toString();
        break;
      case "item_size_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.ITEM_SIZE_LIMIT_EXCEEDED.toString();
        break;
      case "element_size_limit_exceeded":
        messageWrapper = LimitExceededMessageWrapper.ELEMENT_SIZE_LIMIT_EXCEEDED.toString();
        break;
      default:
        String lowerCasedMessage = errorCause.toLowerCase();
        if (lowerCasedMessage.contains("subscribers")) {
          messageWrapper =
              LimitExceededMessageWrapper.TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED.toString();
        } else if (lowerCasedMessage.contains("operations")) {
          messageWrapper = LimitExceededMessageWrapper.OPERATIONS_RATE_LIMIT_EXCEEDED.toString();
        } else if (lowerCasedMessage.contains("throughput")) {
          messageWrapper = LimitExceededMessageWrapper.THROUGHPUT_RATE_LIMIT_EXCEEDED.toString();
        } else if (lowerCasedMessage.contains("request limit")) {
          messageWrapper = LimitExceededMessageWrapper.REQUEST_SIZE_LIMIT_EXCEEDED.toString();
        } else if (lowerCasedMessage.contains("item size")) {
          messageWrapper = LimitExceededMessageWrapper.ITEM_SIZE_LIMIT_EXCEEDED.toString();
        } else if (lowerCasedMessage.contains("element size")) {
          messageWrapper = LimitExceededMessageWrapper.ELEMENT_SIZE_LIMIT_EXCEEDED.toString();
        }
        break;
    }
    return new LimitExceededException(cause, transportErrorDetails, messageWrapper);
  }
}

enum LimitExceededMessageWrapper {
  TOPIC_SUBSCRIPTIONS_LIMIT_EXCEEDED("Topic subscriptions limit exceeded for this account"),
  OPERATIONS_RATE_LIMIT_EXCEEDED("Request rate limit exceeded for this account"),
  THROUGHPUT_RATE_LIMIT_EXCEEDED("Bandwidth limit exceeded for this account"),
  REQUEST_SIZE_LIMIT_EXCEEDED("Request size limit exceeded for this account"),
  ITEM_SIZE_LIMIT_EXCEEDED("Item size limit exceeded for this account"),
  ELEMENT_SIZE_LIMIT_EXCEEDED("Element size limit exceeded for this account"),
  UNKNOWN_LIMIT_EXCEEDED("Limit exceeded for this account");

  private final String messageWrapper;

  /** @param messageWrapper */
  LimitExceededMessageWrapper(final String messageWrapper) {
    this.messageWrapper = messageWrapper;
  }

  /* (non-Javadoc)
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return messageWrapper;
  }
}
