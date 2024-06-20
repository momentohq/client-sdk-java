package momento.sdk.responses.storage;

/** Type of item in a store. */
public enum StorageItemType {
  /** Item is a byte array. */
  BYTE_ARRAY,
  /** Item is a string. */
  STRING,
  /** Item is a long. */
  LONG,
  /** Item is a double. */
  DOUBLE
}
