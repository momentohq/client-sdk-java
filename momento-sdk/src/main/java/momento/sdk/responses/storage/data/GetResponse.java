package momento.sdk.responses.storage.data;

import momento.sdk.exceptions.ClientSdkException;
import momento.sdk.exceptions.SdkException;

/** Response for a get operation */
public interface GetResponse {

  /** A successful get operation. */
  class Success implements GetResponse {
    private final Object value;
    private final ValueType valueType;

    private Success(Object value, ValueType valueType) {
      this.value = value;
      this.valueType = valueType;
    }

    public static Success of(byte[] value) {
      return new Success(value, ValueType.BYTE_ARRAY);
    }

    public static Success of(String value) {
      return new Success(value, ValueType.STRING);
    }

    public static Success of(long value) {
      return new Success(value, ValueType.LONG);
    }

    public static Success of(double value) {
      return new Success(value, ValueType.DOUBLE);
    }

    public ValueType getType() {
      return valueType;
    }

    public byte[] valueByteArray() {
      ensureCorrectTypeOrThrowException(ValueType.BYTE_ARRAY, valueType);
      return (byte[]) value;
    }

    public String valueString() {
      ensureCorrectTypeOrThrowException(ValueType.STRING, valueType);
      return (String) value;
    }

    public long valueLong() {
      ensureCorrectTypeOrThrowException(ValueType.LONG, valueType);
      return (long) value;
    }

    public double valueDouble() {
      ensureCorrectTypeOrThrowException(ValueType.DOUBLE, valueType);
      return (double) value;
    }

    private void ensureCorrectTypeOrThrowException(ValueType requested, ValueType actual) {
      if (requested != actual) {
        // In a regular Java context, ClassCastException or IllegalStateException could be
        // appropriate here
        throw new ClientSdkException(
            String.format(
                "Value is not a %s but was: %s".format(requested.toString(), actual.toString())));
      }
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
  }
}
