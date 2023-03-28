package momento.sdk;

import java.time.Duration;
import momento.sdk.exceptions.InvalidArgumentException;

// Should rely on server for all validations. However, there are some that cannot be delegated and
// instead fail in grpc client, like providing null inputs or a negative ttl.
final class ValidationUtils {

  static final String REQUEST_TIMEOUT_MUST_BE_POSITIVE = "Request timeout must be positive";
  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String A_NON_NULL_KEY_IS_REQUIRED = "A non-null key is required.";
  static final String A_NON_NULL_VALUE_IS_REQUIRED = "A non-null value is required.";
  static final String CACHE_NAME_IS_REQUIRED = "Cache name is required.";
  static final String LIST_NAME_CANNOT_BE_NULL = "Cache name cannot be null.";
  static final String LIST_SLICE_START_END_INVALID =
      "endIndex (exclusive) must be larger than startIndex (inclusive).";
  static final String SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE = "Signing key TTL cannot be negative.";

  ValidationUtils() {}

  static void ensureRequestTimeoutValid(Duration requestTimeout) {
    if (requestTimeout == null || requestTimeout.isNegative() || requestTimeout.isZero()) {
      throw new InvalidArgumentException(REQUEST_TIMEOUT_MUST_BE_POSITIVE);
    }
  }

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new InvalidArgumentException(CACHE_NAME_IS_REQUIRED);
    }
  }

  static void checkListNameValid(String listName) {
    if (listName == null) {
      throw new InvalidArgumentException(LIST_NAME_CANNOT_BE_NULL);
    }
  }

  static void ensureValidCacheSet(Object key, Object value, Duration ttl) {
    ensureValidKey(key);
    if (value == null) {
      throw new InvalidArgumentException(A_NON_NULL_VALUE_IS_REQUIRED);
    }
    ensureValidTtl(ttl);
  }

  static void ensureValidKey(Object key) {
    if (key == null) {
      throw new InvalidArgumentException(A_NON_NULL_KEY_IS_REQUIRED);
    }
  }

  static void ensureValidTtl(Duration ttl) {
    if (ttl.getSeconds() < 0) {
      throw new InvalidArgumentException(CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE);
    }
  }

  static void ensureValidTtlMinutes(Duration ttlMinutes) {
    if (ttlMinutes.toMinutes() < 0) {
      throw new InvalidArgumentException(SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE);
    }
  }
}
