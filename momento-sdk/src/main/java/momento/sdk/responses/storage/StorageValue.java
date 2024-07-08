package momento.sdk.responses.storage;

import momento.sdk.utils.MomentoOptional;

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
 * underlying value is not of the requested type, an exception will be thrown.
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
   * @return the value as an optional byte array. If the value is not a byte array, an empty
   *     optional will be returned. Call {@link MomentoOptional#orElseThrow()} to short circuit the
   *     operation and throw an exception.
   */
  public MomentoOptional<byte[]> getByteArray() {
    if (itemType != StorageItemType.BYTE_ARRAY) {
      return MomentoOptional.empty(formatWrongTypeMessage(StorageItemType.BYTE_ARRAY));
    }
    return MomentoOptional.of((byte[]) value);
  }

  /**
   * Get the value as a string.
   *
   * @return the value as an optional string. If the value is not a string, an empty optional will
   *     be returned. Call {@link MomentoOptional#orElseThrow()} to short circuit the operation and
   *     throw an exception.
   */
  public MomentoOptional<String> getString() {
    if (itemType != StorageItemType.STRING) {
      return MomentoOptional.empty(formatWrongTypeMessage(StorageItemType.STRING));
    }
    return MomentoOptional.of((String) value);
  }

  /**
   * Get the value as a long.
   *
   * @return the value as an optional long. If the value is not a long, an empty optional will be
   *     returned. Call {@link MomentoOptional#orElseThrow()} to short circuit the operation and
   *     throw an exception.
   */
  public MomentoOptional<Long> getLong() {
    if (itemType != StorageItemType.LONG) {
      return MomentoOptional.empty(formatWrongTypeMessage(StorageItemType.LONG));
    }

    return MomentoOptional.of((long) value);
  }

  /**
   * Get the value as a double.
   *
   * @return the value as an optional double. If the value is not a double, an empty optional will
   *     be returned. Call {@link MomentoOptional#orElseThrow()} to short circuit the operation and
   *     throw an exception.
   */
  public MomentoOptional<Double> getDouble() {
    if (itemType != StorageItemType.DOUBLE) {
      return MomentoOptional.empty(formatWrongTypeMessage(StorageItemType.DOUBLE));
    }
    return MomentoOptional.of((double) value);
  }

  private String formatWrongTypeMessage(StorageItemType requested) {
    return String.format(
        "Value is not a %s but was: %s".format(requested.toString(), itemType.toString()));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("StorageValue{");
    sb.append("value=");
    if (itemType == StorageItemType.STRING) {
      sb.append('"');
      sb.append(value);
      sb.append('"');
    } else {
      sb.append(value);
    }
    sb.append(", itemType=");
    sb.append(itemType);
    sb.append('}');
    return sb.toString();
  }
}
