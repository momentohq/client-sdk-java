package momento.sdk.responses.storage.data;

import java.util.Optional;
import momento.sdk.exceptions.SdkException;

/** Response for a get operation */
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

  /** A successful get operation. */
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

    public Optional<StorageValue> value() {
      return value;
    }

    @Override
    public Optional<Success> success() {
      return Optional.of(this);
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
