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
    private final ByteString value;

    public Hit(ByteString value) {
      this.value = value;
    }

    /**
     * Gets the retrieved value as a byte array.
     *
     * @return the value.
     */
    public byte[] valueByteArray() {
      return value.toByteArray();
    }

    /**
     * Gets the retrieved value as a {@link ByteBuffer}.
     *
     * @return the value.
     */
    public ByteBuffer valueByteBuffer() {
      return value.asReadOnlyByteBuffer();
    }

    /**
     * Gets the retrieved value as a UTF-8 {@link String}
     *
     * @return the value.
     */
    public String valueString() {
      return valueString(StandardCharsets.UTF_8);
    }

    /**
     * Gets the retrieved value as a {@link String}.
     *
     * @param charset the string encoding to use.
     * @return the value.
     */
    public String valueString(Charset charset) {
      return value.toString(charset);
    }

    /**
     * Gets the retrieved value as an {@link InputStream}.
     *
     * @return the value.
     */
    public InputStream valueInputStream() {
      return value.newInput();
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
