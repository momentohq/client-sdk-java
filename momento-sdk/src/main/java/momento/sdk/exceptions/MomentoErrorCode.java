package momento.sdk.exceptions;

/**
 * A list of all available Momento error codes. These can be used to check for specific types of
 * errors on a failure response.
 */
public enum MomentoErrorCode {
  /** The client was unable to connect to the server. */
  CONNECTION,

  /** An invalid argument was passed to Momento client. */
  INVALID_ARGUMENT_ERROR,

  /** The service returned an unknown response. */
  UNKNOWN_SERVICE_ERROR,

  /** A cache with the specified name already exists. */
  ALREADY_EXISTS_ERROR,

  /** A cache with the specified name could not be found. */
  NOT_FOUND_ERROR,

  /** An unexpected server error occurred while trying to fulfill the request. */
  INTERNAL_SERVER_ERROR,

  /** Insufficient permissions to perform an operation. */
  PERMISSION_ERROR,

  /** Invalid authentication credentials to connect to the cache service. */
  AUTHENTICATION_ERROR,

  /** The request was cancelled by the server. */
  CANCELLED_ERROR,

  /** Request rate, bandwidth, or object size exceeded the limits for the account. */
  LIMIT_EXCEEDED_ERROR,

  /** The request was invalid. */
  BAD_REQUEST_ERROR,

  /** The client's configured timeout was exceeded. */
  TIMEOUT_ERROR,

  /** The server was temporarily unable to handle the request. */
  SERVER_UNAVAILABLE,

  /** A client resource (most likely memory) was exhausted. */
  CLIENT_RESOURCE_EXHAUSTED,

  /** Unknown or non-specific client-side error. */
  UNKNOWN;
}
