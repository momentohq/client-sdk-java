package momento.sdk.responses.storage;

import java.util.Optional;
import momento.sdk.exceptions.SdkException;

/**
 * Response for a get operation.
 *
 * <p>The response can be either a {@link Success} or an {@link Error}.
 *
 * <p>To shortcut access to the success response, use {@link #success()}. If the operation was
 * successful, the response will be an optional of {@link Success}, otherwise it will be an empty
 * optional.
 *
 * <p>To handle the response otherwise, use pattern matching or an instanceof to check if the
 * response is a {@link Success} or an {@link Error}.
 *
 * <p>Upon a success, the value can be retrieved with {@link Success#value()}. If the value was
 * found in the store, it will be present, otherwise it will be empty.
 */
public interface GetResponse {
  /**
   * Returns the success response if the operation was successful, or an empty optional if the
   * operation failed.
   *
   * <p>This is a convenience method that can be used to avoid instanceof checks and casting.
   *
   * @return The success response, or an empty optional if the operation failed.
   */
  Optional<Success> success();

  /**
   * A successful get operation.
   *
   * <p>To access the value, use {@link #value()}. If the value was found in the store, it will be
   * present, otherwise it will be empty.
   *
   * <p>Use the appropriate type-based accessor on the value to retrieve the value in its
   * corresponding type.
   */
  class Success implements GetResponse {
    private final Optional<StorageValue> value;

    private Success(Optional<StorageValue> value) {
      this.value = value;
    }

    public static Success of() {
      return new Success(Optional.empty());
    }

    public static Success of(byte[] value) {
      return new Success(Optional.of(StorageValue.of(value)));
    }

    public static Success of(String value) {
      return new Success(Optional.of(StorageValue.of(value)));
    }

    public static Success of(long value) {
      return new Success(Optional.of(StorageValue.of(value)));
    }

    public static Success of(double value) {
      return new Success(Optional.of(StorageValue.of(value)));
    }

    /**
     * Returns the value if it exists, or an empty optional if the value does not exist.
     *
     * @return The value, or an empty optional if the value does not exist.
     */
    public Optional<StorageValue> value() {
      return value;
    }

    @Override
    public Optional<Success> success() {
      return Optional.of(this);
    }

    @Override
    public String toString() {
      return super.toString() + ": value: " + value;
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
    public Optional<Success> success() {
      return Optional.empty();
    }
  }
}
