package momento.sdk.messages;

import grpc.cache_client.ECacheResult;
import momento.sdk.exceptions.InternalServerException;

class BaseResponse {
  MomentoCacheResult resultMapper(ECacheResult result) {
    switch (result) {
      case Ok:
        return MomentoCacheResult.Ok;
      case Hit:
        return MomentoCacheResult.Hit;
      case Miss:
        return MomentoCacheResult.Miss;
      default:
        throw new InternalServerException(
            "Unexpected exception occurred while trying to fulfill the request.");
    }
  }
}
