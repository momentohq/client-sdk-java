package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/**
 * Parent response type for a cache setIfNotExists request. The response object is resolved to a
 * type-safe object of one of the following subtypes: {Stored}, {NotStored}, {Error}
 */
public interface CacheSetIfNotExistsResponse {
  /** A successful setIfNotExists operation that set a value. */
  class Stored implements CacheSetIfNotExistsResponse {
    private final ByteString value;
    private final ByteString key;

    /**
     * Indicates that the key did not exist and the value was set for it.
     *
     * @param key The key to which the value is to be stored.
     * @param value The value of the key.
     */
    public Stored(ByteString key, ByteString value) {
      super();
      this.key = key;
      this.value = value;
    }

    /**
     * Gets the retrieved key as a byte array.
     *
     * @return the key.
     */
    public byte[] keyByteArray() {
      return key.toByteArray();
    }

    /**
     * Gets the retrieved key as a UTF-8 {@link String}
     *
     * @return the key.
     */
    public String keyString() {
      return key.toString(StandardCharsets.UTF_8);
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

    /**
     * Gets the retrieved value as a UTF-8 {@link String}
     *
     * @return the value.
     */
    public String value() {
      return valueString();
    }

    @Override
    public String toString() {
      return super.toString()
          + ": keyString: \""
          + StringHelpers.truncate(keyString())
          + "\" keyByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(keyByteArray()))
          + ": valueString: \""
          + StringHelpers.truncate(valueString())
          + "\" valueByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(valueByteArray()))
          + "\"";
    }
  }

  /**
   * A successful setIfNotExists operation that did not store a value because the key was already
   * associated with one.
   */
  class NotStored implements CacheSetIfNotExistsResponse {}

  /**
   * A failed setIfNotExists operation. The response itself is an exception, so it can be directly
   * thrown, or the cause of the error can be retrieved with {@link #getClass()} ()}. The message is
   * a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheSetIfNotExistsResponse {

    /**
     * Constructs a cache setIfNotExists error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
