package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import momento.sdk.exceptions.InternalServerException;

/** Response for a cache get operation */
public final class CacheGetResponse {
  private final ByteString body;
  private final CacheGetStatus status;

  public CacheGetResponse(ECacheResult status, ByteString body) {
    this.body = body;
    this.status = convert(status);
  }

  /**
   * Determine the result of the Get operation.
   *
   * <p>Valid values are {@link CacheGetStatus#HIT} and {@link CacheGetStatus#MISS}.
   *
   * @return The result of Cache Get Operation
   */
  public CacheGetStatus status() {
    return status;
  }

  /**
   * Value stored in the cache as a byte array.
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<byte[]> byteArray() {
    if (status != CacheGetStatus.HIT) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toByteArray());
  }

  /**
   * Value stored in the cache as a {@link ByteBuffer}.
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<ByteBuffer> byteBuffer() {
    if (status != CacheGetStatus.HIT) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.asReadOnlyByteBuffer());
  }

  /**
   * Value stored in the cache as a UTF-8 {@link String}
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<String> string() {
    return string(StandardCharsets.UTF_8);
  }

  /**
   * Value stored in the cache as {@link String}.
   *
   * @param charset to express the bytes as String.
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<String> string(Charset charset) {
    if (status != CacheGetStatus.HIT) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toString(charset));
  }

  /**
   * Value as an {@link InputStream}
   *
   * @return Value stored for the given key. {@link Optional#empty()} if the lookup resulted in a
   *     cache miss.
   */
  public Optional<InputStream> inputStream() {
    if (status != CacheGetStatus.HIT) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.newInput());
  }

  private static CacheGetStatus convert(ECacheResult result) {
    switch (result) {
      case Hit:
        return CacheGetStatus.HIT;
      case Miss:
        return CacheGetStatus.MISS;
      default:
        throw new InternalServerException(
            String.format(
                "Unexpected exception occurred while trying to fulfill the request. Found unsupported Cache result: %s",
                result));
    }
  }
}
