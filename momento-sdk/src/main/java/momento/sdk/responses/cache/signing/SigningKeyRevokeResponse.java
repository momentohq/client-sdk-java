package momento.sdk.responses.cache.signing;

import momento.sdk.exceptions.SdkException;

/** Response for a revoke signing key operation. */
public interface SigningKeyRevokeResponse {

  /** A successful revoke signing key operation. */
  class Success implements SigningKeyRevokeResponse {}

  /**
   * A failed revoke signing key operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements SigningKeyRevokeResponse {

    /**
     * Constructs a signing key revocation error with a cause.
     *
     * @param cause the cause.
     */
    public Error(SdkException cause) {
      super(cause);
    }
  }
}
