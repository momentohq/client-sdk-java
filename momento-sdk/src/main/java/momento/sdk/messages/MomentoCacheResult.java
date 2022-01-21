package momento.sdk.messages;

import grpc.cache_client.ECacheResult;
import momento.sdk.exceptions.InternalServerException;

/** Encapsulates the status of the Cache operation */
public enum MomentoCacheResult {

  /** Status if an item was found in Cache. */
  Hit(ECacheResult.Hit),
  /** Status if an item was not found in Cache. */
  Miss(ECacheResult.Miss);

  private int result;

  MomentoCacheResult(grpc.cache_client.ECacheResult num) {
    this.result = num.getNumber();
  }

  static MomentoCacheResult from(ECacheResult result) {
    switch (result) {
      case Hit:
        return MomentoCacheResult.Hit;
      case Miss:
        return MomentoCacheResult.Miss;
      default:
        throw new InternalServerException(
            String.format(
                "Unexpected exception occurred while trying to fulfill the request. Found unsupported Cache result: %s",
                result));
    }
  }
}
