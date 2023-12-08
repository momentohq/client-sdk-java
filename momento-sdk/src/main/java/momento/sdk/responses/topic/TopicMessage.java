package momento.sdk.responses.topic;

import grpc.cache_client.pubsub._TopicValue;
import java.util.Arrays;
import momento.sdk.exceptions.SdkException;

public interface TopicMessage {

  class Text implements TopicMessage {
    private final String value;
    private final String tokenId;

    public Text(_TopicValue topicValue, String tokenId) {
      this.value = topicValue.getText();
      this.tokenId = tokenId;
    }

    public String getValue() {
      return value;
    }

    public String getTokenId() {
      return tokenId;
    }

    @Override
    public String toString() {
      return "Text{" + "value='" + value + '\'' + ", tokenId='" + tokenId + '\'' + '}';
    }
  }

  class Binary implements TopicMessage {
    private final byte[] value;
    private final String tokenId;

    public Binary(_TopicValue topicValue, String tokenId) {
      this.value = topicValue.getBinary().toByteArray();
      this.tokenId = tokenId;
    }

    public byte[] getValue() {
      return value;
    }

    public String getTokenId() {
      return tokenId;
    }

    @Override
    public String toString() {
      return "Binary{" + "value=" + Arrays.toString(value) + ", tokenId='" + tokenId + '\'' + '}';
    }
  }

  class Error extends SdkException implements TopicMessage {

    /**
     * Constructs a topic message error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
