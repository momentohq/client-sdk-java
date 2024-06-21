package momento.sdk.responses.storage;

import java.util.Optional;
import momento.sdk.internal.StringHelpers;

/**
 * A value stored in the storage.
 *
 * <p>Values can be of the following types:
 *
 * <ul>
 *   <li>byte array
 *   <li>string
 *   <li>long
 *   <li>double
 * </ul>
 *
 * <p>Use the appropriate accessor to retrieve the value in its corresponding type. If the
 * underlying value is not of the requested type, an empty optional will be returned.
 */
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

  /**
   * Get the type of the value.
   *
   * @return the type of the value.
   */
  public StorageItemType getType() {
    return itemType;
  }

  /**
   * Get the value as a byte array.
   *
   * @return the value as a byte array. If the value is not a byte array, an empty optional is
   *     returned.
   */
  public Optional<byte[]> getByteArray() {
    if (itemType != StorageItemType.BYTE_ARRAY) {
      return Optional.empty();
    }
    return Optional.of((byte[]) value);
  }

  /**
   * Get the value as a string.
   *
   * @return the value as a string. If the value is not a string, an empty optional is returned.
   */
  public Optional<String> getString() {
    if (itemType != StorageItemType.STRING) {
      return Optional.empty();
    }
    return Optional.of((String) value);
  }

  /**
   * Get the value as a long.
   *
   * @return the value as a long. If the value is not a long, an empty optional is returned.
   */
  public Optional<Long> getLong() {
    if (itemType != StorageItemType.LONG) {
      return Optional.empty();
    }
    return Optional.of((long) value);
  }

  /**
   * Get the value as a double.
   *
   * @return the value as a double. If the value is not a double, an empty optional is returned.
   */
  public Optional<Double> getDouble() {
    if (itemType != StorageItemType.DOUBLE) {
      return Optional.empty();
    }
    return Optional.of((double) value);
  }

  @Override
  public String toString() {
    return super.toString()
        + ": value: "
        + StringHelpers.truncate(value.toString())
        + ", itemType:"
        + itemType;
  }
}
