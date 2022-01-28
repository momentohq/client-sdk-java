package momento.sdk.messages;

import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/** Result of the set operation on Cache. */
public final class CacheSetResponse {
  private final ByteString value;

  public CacheSetResponse(ByteString value) {
    this.value = value;
  }

  /**
   * Value set in the cache as a byte array.
   *
   * @return Value set for the given key.
   */
  public byte[] byteArray() {
    return value.toByteArray();
  }

  /**
   * Value set in the cache as a {@link ByteBuffer}.
   *
   * @return Value set for the given key.
   */
  public ByteBuffer byteBuffer() {
    return value.asReadOnlyByteBuffer();
  }

  /**
   * Value set in the cache as a UTF-8 {@link String}
   *
   * @return Value set for the given key.
   */
  public String string() {
    return string(StandardCharsets.UTF_8);
  }

  /**
   * Value set in the cache as {@link String}.
   *
   * @param charset to express the bytes as String.
   * @return Value set for the given key.
   */
  public String string(Charset charset) {
    return value.toString(charset);
  }

  /**
   * Value as an {@link InputStream}
   *
   * @return Value set for the given key.
   */
  public InputStream inputStream() {
    return value.newInput();
  }
}
