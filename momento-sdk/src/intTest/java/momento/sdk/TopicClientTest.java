package momento.sdk;

import momento.sdk.config.Configurations;
import momento.sdk.responses.topic.TopicPublishResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicClientTest extends BaseTestClass {
    private static final Duration DEFAULT_TTL = Duration.ofSeconds(60);

    private final String cacheName = System.getenv("TEST_CACHE_NAME");
    private CacheClient cacheClient;
    private TopicClient topicClient;

    private final String topicName = "test-topic";

    @BeforeEach
    void setup() {
        cacheClient =
                CacheClient.builder(credentialProvider, Configurations.Laptop.latest(), DEFAULT_TTL)
                        .build();
        topicClient =
                TopicClient.builder(credentialProvider, Configurations.Laptop.latest())
                        .build();
        cacheClient.createCache(cacheName).join();
    }

    @AfterEach
    void teardown() {
        cacheClient.deleteCache(cacheName).join();
        cacheClient.close();
        topicClient.close();
    }

    @Test
    public void topicPublish() {
        final String value = "test-value";
        final TopicPublishResponse response = topicClient.publish(cacheName, topicName, value).join();
        assertThat(response).isInstanceOf(TopicPublishResponse.Success.class);
    }
}
