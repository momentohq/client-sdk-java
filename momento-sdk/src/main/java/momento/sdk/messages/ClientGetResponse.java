package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

public final class ClientGetResponse<T> extends BaseResponse {
  private final T body;
  private final ECacheResult result;

  public ClientGetResponse(ECacheResult result, T body) {
    this.body = body;
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }

  public T getBody() {
    return body;
  }
}
