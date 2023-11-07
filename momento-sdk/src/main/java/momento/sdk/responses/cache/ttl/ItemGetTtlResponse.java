package momento.sdk.responses.cache.ttl;

import java.time.Duration;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/**
 * Parent response type for a cache itemGetTtl request. The response object is resolved to a
 * type-safe object of one of the following subtypes: {Hit}, {Miss}, {Error}
 */
public interface ItemGetTtlResponse {

    /** A successful ≈ operation for a key that exists. */
    class Hit implements ItemGetTtlResponse {
        private final Duration ttl;

        /**
         * Constructs a cache itemGetTtl hit with a ttl value.
         *
         * @param ttl the retrieved ttl
         */
        public Hit(Duration ttl) {
            this.ttl = ttl;
        }

        /**
         * Gets the retrieved ttl as a Duration.
         *
         * @return the ttl.
         */
        public Duration ttl() {
            return ttl;
        }

        /**
         * Gets the retrieved ttl as milliseconds since epoch
         *
         * @return the ttl.
         */
        public long ttlMillis() {
            return ttl.toMillis();
        }

        /**
         * {@inheritDoc}
         *
         * <p>Truncates the internal fields to 20 characters to bound the size of the string.
         */
        @Override
        public String toString() {
            return super.toString()
                    + ": ttl: \""
                    + StringHelpers.truncate(ttl.toString());
        }
    }

    /** A successful itemGetTtl operation for a non-existent key. */
    class Miss implements ItemGetTtlResponse {}

    /**
     * A failed itemGetTtl operation. The response itself is an exception, so it can be directly thrown, or
     * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
     * message of the cause.
     */
    class Error extends SdkException implements ItemGetTtlResponse {

        /**
         * Constructs a cache itemGetTtl error with a cause.
         *
         * @param cause the cause.
         */
        public Error(SdkException cause) {
            super(cause);
        }
    }
}
