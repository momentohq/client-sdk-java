package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.util.Base64;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a cache dictionary get field operation */
public interface CacheDictionaryGetFieldResponse {
  /**
   * A successful cache dictionary get field operation for a key that has a value in the dictionary.
   */
  class Hit implements CacheDictionaryGetFieldResponse {
    private final ByteString field;
    private final ByteString value;

    /**
     * Constructs a cache dictionary get field hit with an encoded value.
     *
     * @param field the retrieved key.
     * @param value the retrieved value.
     */
    public Hit(ByteString field, ByteString value) {
      this.field = field;
      this.value = value;
    }

    /**
     * Gets the retrieved field as a byte array.
     *
     * @return the field.
     */
    public byte[] fieldByteArray() {
      return field.toByteArray();
    }

    /**
     * Gets the retrieved field as a UTF-8 {@link String}.
     *
     * @return the field.
     */
    public String fieldString() {
      return field.toStringUtf8();
    }

    /**
     * Gets the retrieved field as a UTF-8 {@link String}.
     *
     * @return the field.
     */
    public String field() {
      return field.toStringUtf8();
    }

    /**
     * Gets the retrieved value as a byte array
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
      return value.toStringUtf8();
    }

    /**
     * Gets the retrieved value as a UTF-8 {@link String}
     *
     * @return the value.
     */
    public String value() {
      return valueString();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": fieldString: \""
          + StringHelpers.truncate(fieldString())
          + "\" fieldByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(fieldByteArray()))
          + ": valueString: \""
          + StringHelpers.truncate(valueString())
          + "\" valueByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(valueByteArray()))
          + "\"";
    }
  }

  /**
   * A successful cache dictionary get field operation for a key that does not exist in the
   * dictionary.
   */
  class Miss implements CacheDictionaryGetFieldResponse {
    private final ByteString field;

    public Miss(ByteString field) {
      this.field = field;
    }

    /**
     * Gets the field as a byte array.
     *
     * @return the field.
     */
    public byte[] fieldByteArray() {
      return field.toByteArray();
    }

    /**
     * Gets the field as a UTF-8 {@link String}.
     *
     * @return the field.
     */
    public String fieldString() {
      return field.toStringUtf8();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": fieldString: \""
          + StringHelpers.truncate(fieldString())
          + "\" fieldByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(fieldByteArray()))
          + "\"";
    }
  }

  /**
   * A failed cache dictionary get field operation. The response itself is an exception, so it can
   * be directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements CacheDictionaryGetFieldResponse {
    private final ByteString field;

    /**
     * Constructs a cache dictionary get field error with a cause.
     *
     * @param cause the cause.
     * @param field
     */
    public Error(SdkException cause, ByteString field) {
      super(cause);
      this.field = field;
    }

    /**
     * Gets the field as a byte array.
     *
     * @return the field.
     */
    public byte[] fieldByteArray() {
      return field.toByteArray();
    }

    /**
     * Gets the field as a UTF-8 {@link String}.
     *
     * @return the field.
     */
    public String fieldString() {
      return field.toStringUtf8();
    }

    /**
     * {@inheritDoc}
     *
     * <p>Truncates the internal fields to 20 characters to bound the size of the string.
     */
    @Override
    public String toString() {
      return super.toString()
          + ": fieldString: \""
          + StringHelpers.truncate(fieldString())
          + "\" fieldByteArray: \""
          + StringHelpers.truncate(Base64.getEncoder().encodeToString(fieldByteArray()))
          + "\"";
    }
  }
}
