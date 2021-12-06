package momento.sdk.messages;

public final class CacheInfo {

  private final String name;

  public CacheInfo(String name) {
    this.name = name;
  }

  public String name() {
    return name;
  }
}
