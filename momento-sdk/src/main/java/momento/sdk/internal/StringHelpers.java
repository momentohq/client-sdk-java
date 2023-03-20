package momento.sdk.internal;

/** Helper methods for working with strings. */
public class StringHelpers {

  public static String truncate(String input) {
    return truncate(input, 20);
  }

  public static String truncate(String input, int maxLength) {
    if (input == null) {
      return null;
    }

    if (input.length() < maxLength) {
      return input;
    }

    return input.substring(0, maxLength) + "...";
  }
}
