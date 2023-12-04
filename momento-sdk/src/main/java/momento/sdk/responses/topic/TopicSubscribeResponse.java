package momento.sdk.responses.topic;

import momento.sdk.exceptions.SdkException;

/** Response for a topic subscribe operation */

public interface TopicSubscribeResponse{

    /** A successful topic subscribe operation. */

    class Success implements TopicSubscribeResponse {}

    /**
     * A failed topic subscribe operation. The response itself is an exception, so it can be directly
     * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
     * copy of the message of the cause.
     */
    class Error extends SdkException implements TopicSubscribeResponse {

        /**
         * Constructs a topic publish error with a cause.
         *
         * @param cause the cause.
         */
        public Error(SdkException cause) {
            super(cause);
        }
    }
}
