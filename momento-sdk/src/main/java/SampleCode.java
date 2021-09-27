import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import momento.sdk.Cache;
import momento.sdk.Momento;
import momento.sdk.messages.ClientGetResponse;
import momento.sdk.messages.ClientSetResponse;

public class SampleCode {

  public static void main(String[] args) throws IOException {

    Momento momento = Momento.init("authToken");

    momento.createCache("myCache");

    Cache cache = momento.getCache("myCache");

    ClientSetResponse setResponse =
        cache.set(
            /* key = */ "key",
            /* value = */ ByteBuffer.wrap("value".getBytes(StandardCharsets.UTF_8)),
            /* ttlSeconds = */ 900);

    // TODO: Sample code to handle org.momento.client.messages.ClientSetResponse

    ClientGetResponse getResponse = cache.get("key");

    // TODO: Sample code to handle org.momento.client.messages.ClientGetResponse

  }
}
