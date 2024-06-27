package momento.sdk.internal;

/** Helper methods for working with strings. */
public class StringHelpers {

  /**
   * Truncates a string to 20 characters.
   *
   * @param input The string to truncate.
   * @return The truncated string.
   */
  public static String truncate(String input) {
    return truncate(input, 20);
  }

  /**
   * Truncates a string to the specified number of characters.
   *
   * @param input The string to truncate.
   * @param maxLength The maximum length of the string before truncating with '...'.
   * @return The truncated string.
   */
  public static String truncate(String input, int maxLength) {
    if (input == null) {
      return null;
    }

    if (input.length() < maxLength) {
      return input;
    }

    return input.substring(0, maxLength) + "...";
  }

  public static String emptyToString(String className) {
    return className + "{}";
  }
}
