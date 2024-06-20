package momento.sdk.responses.storage;

import java.util.Optional;

public class StorageValue {
  private final Object value;
  private final StorageItemType itemType;

  private StorageValue(Object value, StorageItemType itemType) {
    this.value = value;
    this.itemType = itemType;
  }

  public static StorageValue of(byte[] value) {
    return new StorageValue(value, StorageItemType.BYTE_ARRAY);
  }

  public static StorageValue of(String value) {
    return new StorageValue(value, StorageItemType.STRING);
  }

  public static StorageValue of(long value) {
    return new StorageValue(value, StorageItemType.LONG);
  }

  public static StorageValue of(double value) {
    return new StorageValue(value, StorageItemType.DOUBLE);
  }

  public StorageItemType getType() {
    return itemType;
  }

  public Optional<byte[]> getByteArray() {
    if (itemType != StorageItemType.BYTE_ARRAY) {
      return Optional.empty();
    }
    return Optional.of((byte[]) value);
  }

  public Optional<String> getString() {
    if (itemType != StorageItemType.STRING) {
      return Optional.empty();
    }
    return Optional.of((String) value);
  }

  public Optional<Long> getLong() {
    if (itemType != StorageItemType.LONG) {
      return Optional.empty();
    }
    return Optional.of((long) value);
  }

  public Optional<Double> getDouble() {
    if (itemType != StorageItemType.DOUBLE) {
      return Optional.empty();
    }
    return Optional.of((double) value);
  }
}
