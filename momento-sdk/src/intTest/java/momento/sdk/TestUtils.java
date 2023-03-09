package momento.sdk;

import java.util.UUID;

public class TestUtils {

  public static String randomString(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }
}
