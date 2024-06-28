package momento.sdk.responses.storage;

import momento.sdk.exceptions.ClientSdkException;

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
   * @return the value as a byte array. If the value is not a byte array, an exception will be
   *     thrown.
   */
  public byte[] getByteArray() {
    ensureCorrectTypeOrThrowException(StorageItemType.BYTE_ARRAY, itemType);
    return (byte[]) value;
  }

  /**
   * Get the value as a string.
   *
   * @return the value as a string. If the value is not a string, an exception will be thrown.
   */
  public String getString() {
    ensureCorrectTypeOrThrowException(StorageItemType.STRING, itemType);
    return (String) value;
  }

  /**
   * Get the value as a long.
   *
   * @return the value as a long. If the value is not a long, an exception will be thrown.
   */
  public long getLong() {
    ensureCorrectTypeOrThrowException(StorageItemType.LONG, itemType);
    return (long) value;
  }

  /**
   * Get the value as a double.
   *
   * @return the value as a double. If the value is not a double, an exception will be thrown.
   */
  public double getDouble() {
    ensureCorrectTypeOrThrowException(StorageItemType.DOUBLE, itemType);
    return (double) value;
  }

  private void ensureCorrectTypeOrThrowException(
      StorageItemType requested, StorageItemType actual) {
    if (requested != actual) {
      // In a regular Java context, ClassCastException or IllegalStateException could be
      // appropriate here.
      throw new ClientSdkException(
          String.format(
              "Value is not a %s but was: %s".format(requested.toString(), actual.toString())));
    }
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
