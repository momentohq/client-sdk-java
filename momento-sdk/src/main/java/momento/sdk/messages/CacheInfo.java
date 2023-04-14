package momento.sdk.messages;

/** Information about a cache. */
public final class CacheInfo {

  private final String name;

  /**
   * Constructs a CacheInfo.
   *
   * @param name the name of the cache.
   */
  public CacheInfo(String name) {
    this.name = name;
  }

  /**
   * Gets the name of the cache.
   *
   * @return the name.
   */
  public String name() {
    return name;
  }
}
