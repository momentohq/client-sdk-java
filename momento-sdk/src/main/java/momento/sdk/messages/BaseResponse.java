package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

// TODO: This should be made package default
public class BaseResponse {
  MomentoCacheResult resultMapper(ECacheResult result) {
    switch (result) {
      case Ok:
        return MomentoCacheResult.Ok;
      case Hit:
        return MomentoCacheResult.Hit;
      case Miss:
        return MomentoCacheResult.Miss;
      case Unauthorized:
        return MomentoCacheResult.Unauthorized;
      case Bad_Request:
        return MomentoCacheResult.Bad_Request;
      case Service_Unavailable:
        return MomentoCacheResult.Service_Unavailable;
      case Internal_Server_Error:
        return MomentoCacheResult.Internal_Server_Error;
      default:
        return MomentoCacheResult.Unknown;
    }
  }
}
