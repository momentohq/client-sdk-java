package momento.client.doc_examples;

import momento.sdk.ISubscriptionCallbacks;
import momento.sdk.TopicClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.TopicConfigurations;
import momento.sdk.responses.topic.TopicMessage;
import momento.sdk.responses.topic.TopicPublishResponse;
import momento.sdk.responses.topic.TopicSubscribeResponse;

public class DocExamplesJavaAPIs {

  @SuppressWarnings("EmptyTryBlock")
  public static void example_API_InstantiateTopicClient() {
    try (final TopicClient topicClient =
        new TopicClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            TopicConfigurations.Laptop.latest())) {
      // ...
    }
  }

  public static void example_API_TopicSubscribe(TopicClient topicClient) {
    final TopicSubscribeResponse topicSubscribeResponse =
        topicClient
            .subscribe(
                "test-cache",
                "test-topic",
                new ISubscriptionCallbacks() {
                  @Override
                  public void onItem(TopicMessage message) {
                    System.out.println("Received message on 'test-topic': " + message.toString());
                  }

                  @Override
                  public void onError(Throwable error) {
                    System.err.println(
                        "Error: Subscription to 'test-topic' failed. Details: "
                            + error.getMessage());
                  }

                  @Override
                  public void onCompleted() {
                    System.out.println("Subscription to 'test-topic' completed");
                  }
                })
            .join();

    if (topicSubscribeResponse instanceof TopicSubscribeResponse.Error error) {
      throw new RuntimeException(
          "An error occurred while attempting to subscribe to topic 'test-topic': "
              + error.getErrorCode(),
          error);
    }
  }

  public static void example_API_TopicPublish(TopicClient topicClient, String message) {
    final TopicPublishResponse publishResponse =
        topicClient.publish("test-cache", "test-topic", message).join();
    if (publishResponse instanceof TopicPublishResponse.Error error) {
      throw new RuntimeException(
          "An error occurred reading messages from topic 'test-topic':" + error.getErrorCode(),
          error);
    }
  }

  public static void main(String[] args) {
    example_API_InstantiateTopicClient();

    try (final TopicClient topicClient =
        new TopicClient(
            CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
            TopicConfigurations.Laptop.latest())) {

      example_API_TopicSubscribe(topicClient);
      example_API_TopicPublish(topicClient, "Hello world");
    }
  }
}
