package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a list pop front operation */
public interface CacheListPopFrontResponse {

  /** A successful list pop front operation that found elements. */
  class Hit implements CacheListPopFrontResponse {
    private ByteString byteStringValue;

    /**
     * Constructs a list pop front hit with encoded value.
     *
     * @param value the retrieved value.
     */
    public Hit(ByteString value) {
      this.byteStringValue = value;
    }

    /**
     * Gets the retrieved value as a byte array.
     *
     * @return the value.
     */
    public byte[] valueByteArray() {
      return this.byteStringValue.toByteArray();
    }

    /**
     * Gets the retrieved value as a UTF-8 String
     *
     * @return the value.
     */
    public String valueString() {
      return this.byteStringValue.toString(StandardCharsets.UTF_8);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": valueString: "
          + StringHelpers.truncate(valueString())
          + " valueByteArray: "
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(valueByteArray()));
    }
  }

  /** A successful list pop front operation that did not find the list. */
  class Miss implements CacheListPopFrontResponse {}

  /**
   * A failed list pop front operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getCause()}. The message is a
   * copy of the message of the cause.
   */
  class Error extends SdkException implements CacheListPopFrontResponse {

    /**
     * Constructs a list pop front error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
