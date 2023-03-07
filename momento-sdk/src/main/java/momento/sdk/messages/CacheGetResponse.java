package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.SdkException;

/** Response for a cache get operation */
public abstract class CacheGetResponse {

  public static class Hit extends CacheGetResponse {
    private final ByteString body;

    public Hit(ByteString body) {
      this.body = body;
    }

    /**
     * Value stored in the cache as a byte array.
     *
     * @return Value stored for the given key.
     */
    public byte[] byteArray() {
      return body.toByteArray();
    }

    /**
     * Value stored in the cache as a {@link ByteBuffer}.
     *
     * @return Value stored for the given key.
     */
    public ByteBuffer byteBuffer() {
      return body.asReadOnlyByteBuffer();
    }

    /**
     * Value stored in the cache as a UTF-8 {@link String}
     *
     * @return Value stored for the given key.
     */
    public String string() {
      return string(StandardCharsets.UTF_8);
    }

    /**
     * Value stored in the cache as {@link String}.
     *
     * @param charset to express the bytes as String.
     * @return Value stored for the given key.
     */
    public String string(Charset charset) {
      return body.toString(charset);
    }

    /**
     * Value as an {@link InputStream}
     *
     * @return Value stored for the given key.
     */
    public InputStream inputStream() {
      return body.newInput();
    }
  }

  public static class Miss extends CacheGetResponse {}

  public static class Error extends CacheGetResponse {
    private final SdkException exception;

    public Error(SdkException exception) {
      this.exception = exception;
    }

    public SdkException exception() {
      return this.exception;
    }
  }
}
