package momento.sdk.responses.cache.control;

/** Information about a cache. */
public final class CacheInfo {

  private final String name;
  private final String id;

  /**
   * Constructs a CacheInfo with an empty id.
   *
   * @param name the name of the cache.
   */
  public CacheInfo(String name) {
    this(name, "");
  }

  /**
   * Constructs a CacheInfo.
   *
   * @param name the name of the cache.
   * @param id the id of the cache.
   */
  public CacheInfo(String name, String id) {
    this.name = name;
    this.id = id;
  }

  /**
   * Gets the name of the cache.
   *
   * @return the name.
   */
  public String name() {
    return name;
  }

  /**
   * Gets the id of the cache.
   *
   * @return the id.
   */
  public String id() {
    return id;
  }
}
