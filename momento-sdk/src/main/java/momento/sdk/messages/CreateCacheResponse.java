package momento.sdk.messages;

import momento.sdk.Cache;

public final class CreateCacheResponse {

  private final Cache cacheClient;

  public CreateCacheResponse(Cache cache) {
    this.cacheClient = cache;
  }

  public Cache cache() {
    return this.cacheClient;
  }
}
