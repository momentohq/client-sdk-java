package momento.sdk.retry.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;

public class TestAdminClient {
  protected final String hostname =
      Optional.ofNullable(System.getenv("TEST_ADMIN_HOSTNAME")).orElse("127.0.0.1");
  protected final int port = Optional.ofNullable(System.getenv("TEST_ADMIN_PORT")).map(Integer::parseInt).orElse(9090);
  private final String endpoint;

  public TestAdminClient() {
    this.endpoint = hostname + ":" + port;
  }

  public void blockPort() throws IOException {
    sendRequest("/block", "Failed to block port");
  }

  public void unblockPort() throws IOException {
    sendRequest("/unblock", "Failed to unblock port");
  }

  private void sendRequest(String path, String errorMessage) throws IOException {
    String urlString = "http://" + this.endpoint + path;
    HttpURLConnection connection = null;

    try {
      URL url = new URL(urlString);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      connection.setConnectTimeout(5000); // 5 seconds timeout
      connection.setReadTimeout(5000); // 5 seconds timeout

      int responseCode = connection.getResponseCode();
      if (responseCode != 200) {
        throw new IOException(errorMessage + ": Received response code " + responseCode);
      }
    } catch (IOException e) {
      LoggerFactory.getLogger(TestAdminClient.class).error(errorMessage);
      throw e;
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
