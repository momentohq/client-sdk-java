package momento.sdk;

import momento.sdk.exceptions.InvalidArgumentException;

final class ValidationUtils {

  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String A_NON_NULL_KEY_IS_REQUIRED = "A non-null Key is required.";
  static final String A_NON_NULL_VALUE_IS_REQUIRED = "A non-null value is required.";
  static final String CACHE_NAME_IS_REQUIRED = "Cache name is required.";

  ValidationUtils() {}

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new InvalidArgumentException(CACHE_NAME_IS_REQUIRED);
    }
  }

  static void ensureValidCacheSet(Object key, Object value, int ttlSeconds) {
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

  static void ensureValidTtl(int ttlSeconds) {
    if (ttlSeconds < 0) {
      throw new InvalidArgumentException(CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE);
    }
  }
}
