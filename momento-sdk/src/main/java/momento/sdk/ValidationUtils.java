package momento.sdk;

import java.time.Duration;
import momento.sdk.exceptions.InvalidArgumentException;

// Should rely on server for all validations. However, there are some that cannot be delegated and
// instead fail in grpc client, like providing null inputs or a negative ttl.
public final class ValidationUtils {

  static final String REQUEST_DEADLINE_MUST_BE_POSITIVE = "Request deadline must be positive";
  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String A_NON_NULL_KEY_IS_REQUIRED = "A non-null key is required.";
  static final String A_NON_NULL_VALUE_IS_REQUIRED = "A non-null value is required.";
  static final String CACHE_NAME_IS_REQUIRED = "Cache name is required.";
  static final String DICTIONARY_NAME_IS_REQUIRED = "Dictionary name is required.";
  static final String SET_NAME_CANNOT_BE_NULL = "Set name cannot be null.";
  static final String LIST_NAME_CANNOT_BE_NULL = "List name cannot be null.";
  static final String LIST_SLICE_START_END_INVALID =
      "endIndex (exclusive) must be larger than startIndex (inclusive).";
  static final String SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE = "Signing key TTL cannot be negative.";

  ValidationUtils() {}

  public static void ensureRequestDeadlineValid(Duration requestDeadline) {
    if (requestDeadline == null || requestDeadline.isNegative() || requestDeadline.isZero()) {
      throw new InvalidArgumentException(REQUEST_DEADLINE_MUST_BE_POSITIVE);
    }
  }

  static void checkCacheNameValid(String cacheName) {
    if (cacheName == null) {
      throw new InvalidArgumentException(CACHE_NAME_IS_REQUIRED);
    }
  }

  static void checkDictionaryNameValid(String dictionaryName) {
    if (dictionaryName == null) {
      throw new InvalidArgumentException(DICTIONARY_NAME_IS_REQUIRED);
    }
  }

  static void checkListNameValid(String listName) {
    if (listName == null) {
      throw new InvalidArgumentException(LIST_NAME_CANNOT_BE_NULL);
    }
  }

  static void checkSetNameValid(String setName) {
    if (setName == null) {
      throw new InvalidArgumentException(SET_NAME_CANNOT_BE_NULL);
    }
  }

  static void checkListSliceStartEndValid(Integer startIndex, Integer endIndex) {
    if (startIndex == null || endIndex == null) return;
    if (endIndex <= startIndex) {
      throw new InvalidArgumentException(LIST_SLICE_START_END_INVALID);
    }
  }

  static void ensureValidCacheSet(Object key, Object value, Duration ttl) {
    ensureValidKey(key);
    ensureValidValue(value);
    ensureValidTtl(ttl);
  }

  static void ensureValidKey(Object key) {
    if (key == null) {
      throw new InvalidArgumentException(A_NON_NULL_KEY_IS_REQUIRED);
    }
  }

  static void ensureValidValue(Object value) {
    if (value == null) {
      throw new InvalidArgumentException(A_NON_NULL_VALUE_IS_REQUIRED);
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
