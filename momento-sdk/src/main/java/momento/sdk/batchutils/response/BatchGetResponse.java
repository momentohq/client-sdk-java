package momento.sdk.batchutils.response;

import momento.sdk.exceptions.SdkException;
import momento.sdk.responses.cache.GetResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Represents the response from a batch get operation in the cache.
 */
public interface BatchGetResponse {

    /**
     * Represents a summary of the batch get operation, encapsulating the results for each key.
     */
    class BatchGetSummary implements BatchGetResponse {

        /**
         * Represents the result of a get operation for a single key within a batch request.
         */
        public static class GetSummary {
            private final String key;
            private final CompletableFuture<GetResponse> getResponse;

            /**
             * Constructs a GetSummary with the provided key and its corresponding get response.
             *
             * @param key         The key for which the get operation was performed.
             * @param getResponse The response from the get operation for the specified key.
             */
            public GetSummary(String key, CompletableFuture<GetResponse> getResponse) {
                this.key = key;
                this.getResponse = getResponse;
            }

            /**
             * Returns the key associated with this summary.
             *
             * @return The key for the get operation.
             */
            public String getKey() {
                return key;
            }

            /**
             * Returns the get response for the key.
             *
             * @return The response from the get operation.
             */
            public CompletableFuture<GetResponse> getGetResponse() {
                return getResponse;
            }
        }

        private final List<GetSummary> summaries;

        /**
         * Constructs a BatchGetSummary with a list of individual get summaries.
         *
         * @param summaries The list of get summaries for each key in the batch request.
         */
        public BatchGetSummary(final List<GetSummary> summaries) {
            this.summaries = summaries;
        }

        /**
         * Returns the list of get summaries in this batch response.
         *
         * @return A list of GetSummary instances.
         */
        public List<GetSummary> getSummaries() {
            return this.summaries;
        }
    }

    /**
     * Represents an error that occurred during a batch get operation.
     */
    class Error extends SdkException implements BatchGetResponse {

        /**
         * Constructs a BatchGetResponse Error with the specified cause.
         *
         * @param cause The cause of the error in the batch get operation.
         */
        public Error(SdkException cause) {
            super(cause);
        }
    }
}
