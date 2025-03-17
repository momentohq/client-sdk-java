package momento.sdk.exceptions;

import java.util.HashMap;
import java.util.Map;

public class MomentoErrorCodeMetadataConverter {
  private static final Map<MomentoErrorCode, String> momentoErrorCodeToMetadataMap =
      new HashMap<>();

  static {
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.INVALID_ARGUMENT_ERROR, "invalid-argument");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.ALREADY_EXISTS_ERROR, "already-exists");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.NOT_FOUND_ERROR, "not-found");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.INTERNAL_SERVER_ERROR, "internal");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.PERMISSION_ERROR, "permission-denied");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.AUTHENTICATION_ERROR, "unauthenticated");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.CANCELLED_ERROR, "cancelled");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.CONNECTION, "unavailable");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.LIMIT_EXCEEDED_ERROR, "deadline-exceeded");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.BAD_REQUEST_ERROR, "invalid-argument");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.TIMEOUT_ERROR, "deadline-exceeded");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.SERVER_UNAVAILABLE, "unavailable");
    momentoErrorCodeToMetadataMap.put(
        MomentoErrorCode.CLIENT_RESOURCE_EXHAUSTED, "resource-exhausted");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.UNKNOWN, "unknown");
    momentoErrorCodeToMetadataMap.put(MomentoErrorCode.UNKNOWN_SERVICE_ERROR, "unknown");
  }

  /**
   * Converts a Momento error code to its corresponding metadata type.
   *
   * @param errorCode The error code to convert.
   * @return The corresponding metadata type.
   * @throws IllegalArgumentException if the error code is not supported.
   */
  public static String convert(MomentoErrorCode errorCode) {
    if (!momentoErrorCodeToMetadataMap.containsKey(errorCode)) {
      throw new IllegalArgumentException("Unsupported MomentoErrorCode: " + errorCode);
    }
    return momentoErrorCodeToMetadataMap.get(errorCode);
  }
}
