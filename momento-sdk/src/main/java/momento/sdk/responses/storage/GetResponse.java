package momento.sdk.responses.storage;

import momento.sdk.exceptions.SdkException;
import momento.sdk.internal.StringHelpers;
import momento.sdk.utils.MomentoOptional;

/**
 * Response for a get operation.
 *
 * <p>The response can be either a {@link Found}, {@link NotFound}, or an {@link Error}.
 *
 * <p>To shortcut access to found value, use {@link #valueWhenFound()}. If the operation was
 * successful but the key was not found, the response will be an empty optional. If the operation
 * failed, the response will also be an empty optional.
 *
 * <p>To handle the response otherwise, use pattern matching or an instanceof to check if the
 * response is a {@link Found}, {@link NotFound}, or an {@link Error}.
 *
 * <p>Upon a found response, the value can be retrieved with {@link Found#value()}.
 */
public interface GetResponse {
  /**
   * Returns the found value if the operation was successful and the key found, or an empty optional
   * if the key was not found or the operation failed.
   *
   * <p>This is a convenience method that can be used to avoid instanceof checks and casting.
   *
   * @return The found value if the operation was successful and the key found, or an empty
   *     optional.
   */
  MomentoOptional<StorageValue> valueWhenFound();

  /**
   * A successful get operation.
   *
   * <p>To access the value, use {@link #value()}. If the value was found in the store, it will be
   * present, otherwise it will be empty.
   *
   * <p>Use the appropriate type-based accessor on the value to retrieve the value in its
   * corresponding type.
   */
  class Found implements GetResponse {
    private final StorageValue value;

    private Found(StorageValue value) {
      this.value = value;
    }

    public static Found of(byte[] value) {
      return new Found(StorageValue.of(value));
    }

    public static Found of(String value) {
      return new Found(StorageValue.of(value));
    }

    public static Found of(long value) {
      return new Found(StorageValue.of(value));
    }

    public static Found of(double value) {
      return new Found(StorageValue.of(value));
    }

    /**
     * Returns the value if it exists, or an empty optional if the value does not exist.
     *
     * @return The value, or an empty optional if the value does not exist.
     */
    public StorageValue value() {
      return value;
    }

    @Override
    public MomentoOptional<StorageValue> valueWhenFound() {
      return MomentoOptional.of(value());
    }

    @Override
    public String toString() {
      return "GetResponse.Found{value=" + value + "}";
    }
  }

  class NotFound implements GetResponse {
    public NotFound() {}

    @Override
    public MomentoOptional<StorageValue> valueWhenFound() {
      return MomentoOptional.empty("Value was not found in the store.");
    }

    @Override
    public String toString() {
      return StringHelpers.emptyToString("GetResponse.NotFound");
    }
  }

  /**
   * A failed get operation. The response itself is an exception, so it can be directly thrown, or
   * the cause of the error can be retrieved with {@link #getCause()}. The message is a copy of the
   * message of the cause.
   */
  class Error extends SdkException implements GetResponse {

    /**
     * Constructs a persistent store get error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }

    @Override
    public MomentoOptional<StorageValue> valueWhenFound() {
      return MomentoOptional.empty("The get operation failed: " + this);
    }

    @Override
    public String toString() {
      return buildToString("GetResponse.Error");
    }
  }
}
