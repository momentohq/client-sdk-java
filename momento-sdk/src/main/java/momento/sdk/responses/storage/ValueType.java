package momento.sdk.responses.storage;

/** Type of value in a store. */
public enum ValueType {
  /** Value is a byte array. */
  BYTE_ARRAY,
  /** Value is a string. */
  STRING,
  /** Value is a long. */
  LONG,
  /** Value is a double. */
  DOUBLE
}
