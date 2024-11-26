package momento.sdk.responses.cache.ttl;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a cache decreaseTtl request. The response object is resolved to a
 * type-safe object of one of the following subtypes: {Set}, {NotSet}, {Miss}, {Error}
 */
public interface DecreaseTtlResponse {

  /** Indicates that the TTL was successfully decreased for an existing key. */
  class Set implements DecreaseTtlResponse {

    /** Constructs a cache decreaseTtl set */
    public Set() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "DecreaseTtlResponse.Set{}";
    }
  }

  /** Indicates that the TTL was not updated due to a failed condition. */
  class NotSet implements DecreaseTtlResponse {

    /** Constructs a cache decreaseTtl not set */
    public NotSet() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "DecreaseTtlResponse.NotSet{}";
    }
  }

  /** Indicates that the requested item was not found in the cache. */
  class Miss implements DecreaseTtlResponse {

    /** Constructs a cache decreaseTtl miss */
    public Miss() {}

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return "DecreaseTtlResponse.Miss{}";
    }
  }

  /**
   * A failed decreaseTtl operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements DecreaseTtlResponse {

    /**
     * Constructs a cache decreaseTtl error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
