package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

public enum MomentoCacheResult {
  Internal_Server_Error(ECacheResult.Internal_Server_Error),
  Ok(ECacheResult.Ok),
  Hit(ECacheResult.Hit),
  Miss(ECacheResult.Miss),
  Bad_Request(ECacheResult.Bad_Request),
  Unauthorized(ECacheResult.Unauthorized),
  Service_Unavailable(ECacheResult.Service_Unavailable),
  Unknown(65535);

  private int result;

  MomentoCacheResult(grpc.cache_client.ECacheResult num) {
    this.result = num.getNumber();
  }

  MomentoCacheResult(int num) {
    this.result = num;
  }

  public int getResult() {
    return this.result;
  }
}
