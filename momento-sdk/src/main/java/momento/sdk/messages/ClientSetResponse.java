package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

public class ClientSetResponse extends BaseResponse {
  private ECacheResult result;

  public ClientSetResponse(ECacheResult result) {
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }
}
