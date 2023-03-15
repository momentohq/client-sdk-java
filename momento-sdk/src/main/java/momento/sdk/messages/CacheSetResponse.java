package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import momento.sdk.exceptions.SdkException;
import momento.sdk.exceptions.WrappedSdkException;
import momento.sdk.internal.StringHelpers;

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

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": valueString: \""
          + StringHelpers.truncate(valueString())
          + "\" valueByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(valueByteArray()))
          + "\"";
    }
  }

  /**
   * A failed set operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends WrappedSdkException implements CacheSetResponse {

    public Error(SdkException cause) {
      super(cause);
    }
  }
}
