package momento.sdk.exceptions;

/**
 * A list of all available message wrappers for the LimitExceededException. These messages specify
 * which limit was exceeded for the account.
 */
public enum LimitExceededMessageWrapper {
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
