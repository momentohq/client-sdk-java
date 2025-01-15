package momento.sdk;

import java.time.Duration;
import java.util.Map;
import momento.sdk.auth.accessControl.ExpiresIn;
import momento.sdk.exceptions.InvalidArgumentException;

/**
 * Client-side validation methods. While we should rely on server for all validations, there are
 * some that cannot be delegated and instead fail in grpc client, like providing null inputs or a
 * negative ttl.
 */
public final class ValidationUtils {

  static final String REQUEST_DEADLINE_MUST_BE_POSITIVE = "Request deadline must be positive";
  static final String CACHE_ITEM_TTL_CANNOT_BE_NEGATIVE = "Cache item TTL cannot be negative.";
  static final String CANNOT_BE_NULL = " cannot be null.";
  static final String INDEX_RANGE_INVALID =
      "endIndex (exclusive) must be larger than startIndex (inclusive).";
  static final String SCORE_RANGE_INVALID =
      "maxScore (inclusive) must be greater than or equal to minScore (inclusive).";
  static final String SIGNING_KEY_TTL_CANNOT_BE_NEGATIVE = "Signing key TTL cannot be negative.";
  static final String TRUNCATE_TO_SIZE_MUST_BE_POSITIVE = "Truncate-to-size must be positive";

  static final String DISPOSABLE_TOKEN_EXPIRY_MUST_BE_POSITIVE =
      "Disposable token expiry must be positive";
  static final String DISPOSABLE_TOKEN_EXPIRY_EXCEEDS_ONE_HOUR =
      "Disposable token must expire within 1 hour";
  static final String DISPOSABLE_TOKEN_MUST_HAVE_AN_EXPIRY =
      "Disposable tokens must have an expiry";
  static final String LEADERBOARD_NAME_IS_REQUIRED = "Non-empty leaderboard name is required.";
  static final String LEADERBOARD_ELEMENTS_NON_EMPTY = "Leaderboard elements cannot be empty.";
  static final String RANK_RANGE_INVALID =
      "Ranks must not be negative and endRank (exclusive) must be larger than startRank (inclusive).";

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
    validateNotNull(cacheName, "Cache name");
  }

  static void checkStoreNameValid(String storeName) {
    validateNotNull(storeName, "Store name");
  }

  static void checkTopicNameValid(String topicName) {
    validateNotNull(topicName, "Topic name");
  }

  static void checkDictionaryNameValid(String dictionaryName) {
    validateNotNull(dictionaryName, "Dictionary name");
  }

  static void checkListNameValid(byte[] listName) {
    validateNotNull(listName, "List name");
  }

  static void checkListNameValid(String listName) {
    validateNotNull(listName, "List name");
  }

  static void checkSetNameValid(String setName) {
    validateNotNull(setName, "Set name");
  }

  static void checkSortedSetNameValid(String sortedSetName) {
    validateNotNull(sortedSetName, "Sorted set name");
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

  static void validateOffset(Integer offset) {
    if (offset != null && offset < 0) {
      throw new InvalidArgumentException("Offset must be greater than or equal to 0.");
    }
  }

  static void validateCount(Integer count) {
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
    validateNotNull(key, "Key");
  }

  static void ensureValidValue(Object value) {
    validateNotNull(value, "Value");
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

  static void checkValidDisposableTokenExpiry(ExpiresIn expiresIn) {
    if (!expiresIn.doesExpire()) {
      throw new InvalidArgumentException(DISPOSABLE_TOKEN_MUST_HAVE_AN_EXPIRY);
    } else if (expiresIn.getSeconds() > 60 * 60) {
      throw new InvalidArgumentException(DISPOSABLE_TOKEN_EXPIRY_EXCEEDS_ONE_HOUR);
    } else if (expiresIn.getSeconds() <= 0) {
      throw new InvalidArgumentException(DISPOSABLE_TOKEN_EXPIRY_MUST_BE_POSITIVE);
    }
  }

  static void validateLeaderboardName(String leaderboardName) {
    if (leaderboardName == null || leaderboardName.isEmpty()) {
      throw new InvalidArgumentException(LEADERBOARD_NAME_IS_REQUIRED);
    }
  }

  static void validateLeaderboardElements(Map<Integer, Double> elements) {
    if (elements == null || elements.isEmpty()) {
      throw new InvalidArgumentException(LEADERBOARD_ELEMENTS_NON_EMPTY);
    }
  }

  static void validateRankRange(int startRank, int endRank) {
    if (startRank < 0 || endRank < 0 || endRank <= startRank) {
      throw new InvalidArgumentException(RANK_RANGE_INVALID);
    }
  }

  static void validateNotNull(Object object, String name) {
    if (object == null) {
      throw new InvalidArgumentException(name + CANNOT_BE_NULL);
    }
  }
}
