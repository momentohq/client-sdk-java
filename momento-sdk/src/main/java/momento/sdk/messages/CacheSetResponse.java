package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

/** Result of the set operation on Cache. */
public final class CacheSetResponse extends BaseResponse {
  private final ECacheResult result;

  public CacheSetResponse(ECacheResult result) {
    this.result = result;
  }

  /**
   * Result of the Set Operation on Cache.
   *
   * @return result of the set operation. {@link MomentoCacheResult#Ok} is the only valid status.
   *     All other results should be considered an error.
   */
  public MomentoCacheResult result() {
    return this.resultMapper(this.result);
  }
}
