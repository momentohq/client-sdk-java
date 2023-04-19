package momento.sdk;

import java.time.Duration;
import momento.sdk.exceptions.InvalidArgumentException;

/**
 * Client-side validation methods. While we should rely on server for all validations, there are
 * some that cannot be delegated and instead fail in grpc client, like providing null inputs or a
 * negative ttl.
 */
public final class ValidationUtils {

  static final String REQUEST_DEADLINE_MUST_BE_POSITIVE = "Request deadline must be positive";
  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String A_NON_NULL_KEY_IS_REQUIRED = "A non-null key is required.";
  static final String A_NON_NULL_VALUE_IS_REQUIRED = "A non-null value is required.";
  static final String CACHE_NAME_IS_REQUIRED = "Cache name is required.";
  static final String DICTIONARY_NAME_IS_REQUIRED = "Dictionary name is required.";
  static final String SET_NAME_CANNOT_BE_NULL = "Set name cannot be null.";
  static final String LIST_NAME_CANNOT_BE_NULL = "List name cannot be null.";
  static final String INDEX_RANGE_INVALID =
      "endIndex (exclusive) must be larger than startIndex (inclusive).";
  static final String SCORE_RANGE_INVALID =
      "maxScore (inclusive) must be greater than or equal to minScore (inclusive).";
  static final String SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE = "Signing key TTL cannot be negative.";
  static final String TRUNCATE_TO_SIZE_MUST_BE_POSITIVE = "Truncate-to-size must be positive";

  ValidationUtils() {}

  /**
   * Throws an {@link InvalidArgumentException} if the deadline is null or not positive.
   *
   * @param requestDeadline The deadline to validate.
   */
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

  static void checkIndexRangeValid(Integer startIndex, Integer endIndex) {
    if (startIndex == null || endIndex == null) return;
    if (endIndex <= startIndex) {
      throw new InvalidArgumentException(INDEX_RANGE_INVALID);
    }
  }

  static void checkScoreRangeValid(Double minScore, Double maxScore) {
    if (minScore == null || maxScore == null) {
      return;
    }
    if (maxScore < minScore) {
      throw new InvalidArgumentException(SCORE_RANGE_INVALID);
    }
  }

  static void checkSortedSetOffsetValid(Integer offset) {
    if (offset != null && offset < 0) {
      throw new InvalidArgumentException("Offset must be greater than or equal to 0.");
    }
  }

  static void checkSortedSetCountValid(Integer count) {
    if (count != null && count <= 0) {
      throw new InvalidArgumentException("Count must be greater than 0.");
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

  static void ensureValidTruncateToSize(Integer truncateToSize) {
    if (truncateToSize != null && truncateToSize <= 0) {
      throw new InvalidArgumentException(TRUNCATE_TO_SIZE_MUST_BE_POSITIVE);
    }
  }
}
