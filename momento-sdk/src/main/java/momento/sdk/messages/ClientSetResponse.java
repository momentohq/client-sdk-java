package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

public final class ClientSetResponse extends BaseResponse {
  private final ECacheResult result;

  public ClientSetResponse(ECacheResult result) {
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }
}
