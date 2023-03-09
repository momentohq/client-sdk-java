package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import momento.sdk.exceptions.SdkException;

/** Response for a cache get operation */
public interface CacheGetResponse {

  class Hit implements CacheGetResponse {
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

  class Miss implements CacheGetResponse {}

  class Error extends SdkException implements CacheGetResponse {

    public Error(SdkException cause) {
      super(cause.getMessage(), cause);
    }
  }
}
