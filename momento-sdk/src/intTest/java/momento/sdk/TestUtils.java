package momento.sdk;

import java.util.UUID;

public class TestUtils {

  public static String randomString(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  public static String randomString() {
    return UUID.randomUUID().toString();
  }

  public static byte[] randomBytes() {
    return UUID.randomUUID().toString().getBytes();
  }
}
