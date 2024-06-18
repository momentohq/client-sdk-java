package momento.sdk.responses.storage.control;

/** Information about a store. */
public class StoreInfo {
  private final String name;

  public StoreInfo(String name) {
    this.name = name;
  }

  /**
   * Get the name of the store.
   *
   * @return the name of the store.
   */
  public String getName() {
    return name;
  }
}
