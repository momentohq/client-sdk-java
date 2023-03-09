package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.SdkException;

/** Response for a cache get operation */
public interface CacheGetResponse {

  class Hit implements CacheGetResponse {
    private final ByteString value;

    public Hit(ByteString value) {
      this.value = value;
    }

    /**
     * Gets the retrieved value as a byte array.
     *
     * @return the value.
     */
    public byte[] valueByteArray() {
      return value.toByteArray();
    }

    /**
     * Gets the retrieved value as a UTF-8 {@link String}
     *
     * @return the value.
     */
    public String valueString() {
      return value.toString(StandardCharsets.UTF_8);
    }
  }

  class Miss implements CacheGetResponse {}

  class Error extends SdkException implements CacheGetResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
