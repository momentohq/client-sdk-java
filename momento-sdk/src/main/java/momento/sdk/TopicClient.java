package momento.sdk;

import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.Configuration;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/** Client to perform operations against the Momento Cache Service */
public final class TopicClient implements Closeable {
    private final ScsTopicClient scsTopicClient;

    private final Logger logger = LoggerFactory.getLogger(CacheClient.class);

    /**
     * Constructs a TopicClient.
     *
     * @param credentialProvider Provider for the credentials required to connect to Momento.
     * @param configuration Configuration object containing all tunable client settings.
     */
    public TopicClient(
            @Nonnull CredentialProvider credentialProvider,
            @Nonnull Configuration configuration) {
        this.scsTopicClient = new ScsTopicClient(credentialProvider, configuration);
        logger.info("Creating Momento Topic Client");
        logger.debug("Cache endpoint: " + credentialProvider.getCacheEndpoint());
        logger.debug("Control endpoint: " + credentialProvider.getControlEndpoint());
    }

    /**
     * Constructs a TopicClient.
     *
     * @param credentialProvider Provider for the credentials required to connect to Momento.
     * @param configuration Configuration object containing all tunable client settings.
     * @return TopicClient
     */
    public static TopicClient create(
            @Nonnull CredentialProvider credentialProvider,
            @Nonnull Configuration configuration) {
        return create(
                credentialProvider, configuration);
    }

    /**
     * Creates a CacheClient builder.
     *
     * @param credentialProvider Provider for the credentials required to connect to Momento.
     * @param configuration Configuration object containing all tunable client settings.
     * @return The builder.
     */
    public static TopicClientBuilder builder(
            CredentialProvider credentialProvider, Configuration configuration) {
        return new TopicClientBuilder(credentialProvider, configuration);
    }

    /**
     * Publish a message to a topic with provided topic name in a cache with provided cache name.
     *
     * @param cacheName The name of the cache where topic resides.
     * @param topicName The name of the topic.
     * @param message The message to be published.
     * @return A future containing the result of the topic publish: {@link
     *     TopicPublishResponse.Success} or {@link TopicPublishResponse.Error}.
     */
    public CompletableFuture<TopicPublishResponse> publish(String cacheName, String topicName, String message) {
        return scsTopicClient.publish(cacheName, topicName, message);
    }

    /**
     * Publish a message to a topic with provided topic name in a cache with provided cache name.
     *
     * @param cacheName The name of the cache where topic resides.
     * @param topicName The name of the topic.
     * @param message The message to be published.
     * @return A future containing the result of the topic publish: {@link
     *     TopicPublishResponse.Success} or {@link TopicPublishResponse.Error}.
     */
    public CompletableFuture<TopicPublishResponse> publish(String cacheName, String topicName, byte[] message) {
        return scsTopicClient.publish(cacheName, topicName, message);
    }

    /**
     * Subscribe to a topic with provided topic name in a cache with provided cache name.
     *
     * @param cacheName The name of the cache where topic resides.
     * @param topicName The name of the topic.
     * @param resumeAtSequenceNumber The sequence number of the last message.
     *                               The topic will resume from this sequence number.
     * @return A future containing the result of the topic subscribe: {@link
     *     TopicSubscribeResponse.Success} or {@link TopicSubscribeResponse.Error}.
     */
    public CompletableFuture<TopicSubscribeResponse> subscribe(String cacheName, String topicName, Long resumeAtSequenceNumber) {
        return scsTopicClient.subscribe(cacheName, topicName, resumeAtSequenceNumber);
    }

    /**
     * Subscribe to a topic with provided topic name in a cache with provided cache name.
     *
     * @param cacheName The name of the cache where topic resides.
     * @param topicName The name of the topic.
     * @return A future containing the result of the topic subscribe: {@link
     *     TopicSubscribeResponse.Success} or {@link TopicSubscribeResponse.Error}.
     */
    public CompletableFuture<TopicSubscribeResponse> subscribe(String cacheName, String topicName) {
        return scsTopicClient.subscribe(cacheName, topicName);
    }

    @Override
    public void close() {
        scsTopicClient.close();
    }
}
