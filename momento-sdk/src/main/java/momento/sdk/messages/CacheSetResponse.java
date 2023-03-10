package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.SdkException;

/** Response for a cache set operation */
public interface CacheSetResponse {

  /** A successful set operation. Contains the value that was written. */
  class Success implements CacheSetResponse {
    private final ByteString value;

    public Success(ByteString value) {
      this.value = value;
    }

    /**
     * Gets the value set in the cache as a byte array.
     *
     * @return the value.
     */
    public byte[] valueByteArray() {
      return value.toByteArray();
    }

    /**
     * Gets the value set in the cache as a UTF-8 {@link String}
     *
     * @return the value.
     */
    public String valueString() {
      return value.toString(StandardCharsets.UTF_8);
    }
  }

  /**
   * A failed set operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements CacheSetResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
