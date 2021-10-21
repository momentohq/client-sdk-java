package momento.sdk.messages;

import grpc.cache_client.ECacheResult;

/** Encapsulates the status of the Cache operation */
public enum MomentoCacheResult {
  /**
   * @deprecated - Experimental and will be deprecated soon.
   *     <p>SDK will throw an appropriate sub-class of {@link
   *     momento.sdk.exceptions.CacheServiceException} once the switch is done.
   */
  @Deprecated
  Internal_Server_Error(ECacheResult.Internal_Server_Error),
  /** Status when set operation succeeds. */
  Ok(ECacheResult.Ok),
  /** Status if an item was found in Cache. */
  Hit(ECacheResult.Hit),
  /** Status if an item was not found in Cache. */
  Miss(ECacheResult.Miss),
  /**
   * @deprecated - Experimental and will be deprecated soon.
   *     <p>SDK will throw an appropriate sub-class of {@link
   *     momento.sdk.exceptions.CacheServiceException} once the switch is done.
   */
  @Deprecated
  Bad_Request(ECacheResult.Bad_Request),
  /**
   * @deprecated - Experimental and will be deprecated soon.
   *     <p>SDK will throw an appropriate sub-class of {@link
   *     momento.sdk.exceptions.CacheServiceException} once the switch is done.
   */
  @Deprecated
  Unauthorized(ECacheResult.Unauthorized),
  /**
   * @deprecated - Experimental and will be deprecated soon.
   *     <p>SDK will throw an appropriate sub-class of {@link
   *     momento.sdk.exceptions.CacheServiceException} once the switch is done.
   */
  @Deprecated
  Service_Unavailable(ECacheResult.Service_Unavailable),
  /**
   * @deprecated - Experimental and will be deprecated soon.
   *     <p>SDK will throw an appropriate sub-class of {@link
   *     momento.sdk.exceptions.CacheServiceException} once the switch is done.
   */
  @Deprecated
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
