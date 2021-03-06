package momento.sdk.messages;

/** Encapsulates the status of the Cache operation */
public enum CacheGetStatus {
  /** Status if an item was found in Cache. */
  HIT,
  /** Status if an item was not found in Cache. */
  MISS
}
