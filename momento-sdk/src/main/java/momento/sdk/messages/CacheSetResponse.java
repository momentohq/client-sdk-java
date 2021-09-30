package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

public final class CacheSetResponse extends BaseResponse {
  private final ECacheResult result;

  public CacheSetResponse(ECacheResult result) {
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }
}
