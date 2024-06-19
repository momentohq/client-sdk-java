package momento.sdk.responses.storage.data;

import java.util.Optional;

public class StorageValue {
  private final Object value;
  private final ValueType valueType;

  private StorageValue(Object value, ValueType valueType) {
    this.value = value;
    this.valueType = valueType;
  }

  public static StorageValue of(byte[] value) {
    return new StorageValue(value, ValueType.BYTE_ARRAY);
  }

  public static StorageValue of(String value) {
    return new StorageValue(value, ValueType.STRING);
  }

  public static StorageValue of(long value) {
    return new StorageValue(value, ValueType.LONG);
  }

  public static StorageValue of(double value) {
    return new StorageValue(value, ValueType.DOUBLE);
  }

  public ValueType getType() {
    return valueType;
  }

  public Optional<byte[]> getByteArray() {
    if (valueType != ValueType.BYTE_ARRAY) {
      return Optional.empty();
    }
    return Optional.of((byte[]) value);
  }

  public Optional<String> getString() {
    if (valueType != ValueType.STRING) {
      return Optional.empty();
    }
    return Optional.of((String) value);
  }

  public Optional<Long> getLong() {
    if (valueType != ValueType.LONG) {
      return Optional.empty();
    }
    return Optional.of((long) value);
  }

  public Optional<Double> getDouble() {
    if (valueType != ValueType.DOUBLE) {
      return Optional.empty();
    }
    return Optional.of((double) value);
  }
}
