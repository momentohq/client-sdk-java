package momento.sdk.messages;

import momento.sdk.exceptions.SdkException;

/** Response for a revoke signing key operation. */
public interface RevokeSigningKeyResponse {

  /** A successful revoke signing key operation. */
  class Success implements RevokeSigningKeyResponse {}

  /**
   * A failed revoke signing key operation. The response itself is an exception, so it can be
   * directly thrown, or the cause of the error can be retrieved with {@link #getCause()}. The
   * message is a copy of the message of the cause.
   */
  class Error extends SdkException implements RevokeSigningKeyResponse {

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
