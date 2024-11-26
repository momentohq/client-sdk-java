package momento.sdk.responses.cache.ttl;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a cache increaseTtl request. The response object is resolved to a
 * type-safe object of one of the following subtypes: {Set}, {NotSet}, {Miss}, {Error}
 */
public interface IncreaseTtlResponse {

  /** Indicates that the TTL was successfully increased for an existing key. */
  class Set implements IncreaseTtlResponse {

    /** Constructs a cache increaseTtl set */
    public Set() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "IncreaseTtlResponse.Set{}";
    }
  }

  /** Indicates that the TTL was not updated due to a failed condition. */
  class NotSet implements IncreaseTtlResponse {

    /** Constructs a cache increaseTtl not set */
    public NotSet() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "IncreaseTtlResponse.NotSet{}";
    }
  }

  /** Indicates that the requested item was not found in the cache. */
  class Miss implements IncreaseTtlResponse {

    /** Constructs a cache increaseTtl miss */
    public Miss() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "IncreaseTtlResponse.Miss{}";
    }
  }

  /**
   * A failed increaseTtl operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements IncreaseTtlResponse {

    /**
     * Constructs a cache increaseTtl error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
