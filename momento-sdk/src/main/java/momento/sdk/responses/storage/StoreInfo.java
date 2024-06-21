package momento.sdk.responses.storage;

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

  @Override
  public String toString() {
    return super.toString() + ": name: " + name;
  }
}
