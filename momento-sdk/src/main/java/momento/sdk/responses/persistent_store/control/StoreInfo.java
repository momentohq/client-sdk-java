package momento.sdk.responses.persistent_store.control;

public class StoreInfo {
  private final String name;

  public StoreInfo(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
