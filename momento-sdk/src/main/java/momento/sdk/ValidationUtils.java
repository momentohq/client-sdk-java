package momento.sdk;

import momento.sdk.exceptions.InvalidArgumentException;

// Should rely on server for all validations. However, there are some that cannot be delegated and
// instead fail in grpc client, like providing null inputs or a negative ttl.
final class ValidationUtils {

  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String A_NON_NULL_KEY_IS_REQUIRED = "A non-null key is required.";
  static final String A_NON_NULL_VALUE_IS_REQUIRED = "A non-null value is required.";
  static final String CACHE_NAME_IS_REQUIRED = "Cache name is required.";
  static final String SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE = "Signing key TTL cannot be negative.";

  ValidationUtils() {}

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new InvalidArgumentException(CACHE_NAME_IS_REQUIRED);
    }
  }

  static void ensureValidCacheSet(Object key, Object value, long ttlSeconds) {
    ensureValidKey(key);
    if (value == null) {
      throw new InvalidArgumentException(A_NON_NULL_VALUE_IS_REQUIRED);
    }
    ensureValidTtl(ttlSeconds);
  }

  static void ensureValidKey(Object key) {
    if (key == null) {
      throw new InvalidArgumentException(A_NON_NULL_KEY_IS_REQUIRED);
    }
  }

  static void ensureValidTtl(long ttlSeconds) {
    if (ttlSeconds < 0) {
      throw new InvalidArgumentException(CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE);
    }
  }

  static void ensureValidTtlMinutes(int ttlMinutes) {
    if (ttlMinutes < 0) {
      throw new InvalidArgumentException(SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE);
    }
  }
}
