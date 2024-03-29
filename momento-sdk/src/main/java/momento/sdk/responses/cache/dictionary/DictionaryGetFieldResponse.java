package momento.sdk.responses.cache.dictionary;

import com.google.protobuf.ByteString;
import java.util.Base64;
import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;

/** Response for a cache dictionary get field operation */
public interface DictionaryGetFieldResponse {
  /**
   * A successful cache dictionary get field operation for a key that has a value in the dictionary.
   */
  class Hit implements DictionaryGetFieldResponse {
    private final ByteString field;
    private final ByteString value;

    /**
     * Constructs a cache dictionary get field hit with an encoded field and value.
     *
     * @param field the retrieved field.
     * @param value the retrieved value.
     */
    public Hit(ByteString field, ByteString value) {
      this.field = field;
      this.value = value;
    }

    /**
     * Gets the retrieved field as a UTF-8 string.
     *
     * @return The field.
     */
    public String fieldString() {
      return field.toStringUtf8();
    }

    /**
     * Gets the retrieved field as a UTF-8 string.
     *
     * @return The field.
     */
    public String field() {
      return field.toStringUtf8();
    }

    /**
     * Gets the retrieved value as a byte array.
     *
     * @return The value.
     */
    public byte[] valueByteArray() {
      return value.toByteArray();
    }

    /**
     * Gets the retrieved value as a UTF-8 string.
     *
     * @return The value.
     */
    public String valueString() {
      return value.toStringUtf8();
    }

    /**
     * Gets the retrieved value as a UTF-8 string.
     *
     * @return The value.
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
  class Miss implements DictionaryGetFieldResponse {
    private final ByteString field;

    /**
     * Constructs a dictionary get field miss.
     *
     * @param field The field that no value could be found for.
     */
    public Miss(ByteString field) {
      this.field = field;
    }

    /**
     * Gets the field as a byte array.
     *
     * @return the Tield.
     */
    public byte[] fieldByteArray() {
      return field.toByteArray();
    }

    /**
     * Gets the field as a UTF-8 string.
     *
     * @return the Field.
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
  class Error extends SdkException implements DictionaryGetFieldResponse {

    /** The field that failed to be retrieved. */
    private final ByteString field;

    /**
     * Constructs a cache dictionary get field error with a cause.
     *
     * @param cause the cause.
     * @param field the field that failed to be retrieved.
     */
    public Error(SdkException cause, ByteString field) {
      super(cause);
      this.field = field;
    }

    /**
     * Gets the field as a byte array.
     *
     * @return the Field.
     */
    public byte[] fieldByteArray() {
      return field.toByteArray();
    }

    /**
     * Gets the field as a UTF-8 string.
     *
     * @return the Field.
     */
    public String fieldString() {
      return field.toStringUtf8();
    }

    /**
     * Gets the field as a UTF-8 string.
     *
     * @return The field.
     */
    public String field() {
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
