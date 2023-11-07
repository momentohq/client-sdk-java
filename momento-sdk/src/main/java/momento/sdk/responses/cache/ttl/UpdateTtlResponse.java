package momento.sdk.responses.cache.ttl;

import momento.sdk.exceptions.SdkException;

/**
 * Parent response type for a cache updateTTL request. The response object is resolved to a
 * type-safe object of one of the following subtypes: {Set}, {Miss}, {Error}
 */
public interface UpdateTtlResponse {


    /** A successful updateTTL operation for an existent key. */
    class Set implements UpdateTtlResponse {

        /**
         * Constructs a cache updateTTL set
         *
         */
        public Set() {}

        /**
         * {@inheritDoc}
         *
         * <p>Truncates the internal fields to 20 characters to bound the size of the string.
         */
        @Override
        public String toString() {
            return super.toString();
        }
    }

    /** A successful updateTTL operation for a non-existent key. */
    class Miss implements UpdateTtlResponse {}

    /**
     * A failed updateTTL operation. The response itself is an exception, so it can be directly thrown, or
     * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
     * message of the cause.
     */
    class Error extends SdkException implements UpdateTtlResponse {

        /**
         * Constructs a cache updateTTL error with a cause.
         *
         * @param cause the cause.
         */
        public Error(SdkException cause) {
            super(cause);
        }
    }
}
