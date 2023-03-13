package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.SdkException;

/** Response for a cache get operation */
public interface CacheGetResponse {

  /** A successful get operation for a key that has a value. */
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

  /** A successful get operation for a key that has no value. */
  class Miss implements CacheGetResponse {}

  /**
   * A failed get operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements CacheGetResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
