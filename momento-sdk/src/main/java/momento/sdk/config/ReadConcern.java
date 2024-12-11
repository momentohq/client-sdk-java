package momento.sdk.config;

/**
 * The read consistency setting for the cache client. Consistent guarantees read after write
 * consistency, but applies a 6x multiplier to your operation usage.
 */
public enum ReadConcern {
  /**
   * Balanced read concern makes no consistency guarantee. The default read concern for the cache
   * client.
   */
  BALANCED,
  /** Consistent read concern guarantees read after write consistency. */
  CONSISTENT;

  public String toLowerCase() {
    return name().toLowerCase();
  }
}
