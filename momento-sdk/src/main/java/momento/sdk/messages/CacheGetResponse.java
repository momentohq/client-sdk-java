package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Optional;

public final class CacheGetResponse extends BaseResponse {
  private final ByteString body;
  private final ECacheResult result;

  public CacheGetResponse(ECacheResult result, ByteString body) {
    this.body = body;
    this.result = result;
  }

  public MomentoCacheResult result() {
    return this.resultMapper(this.result);
  }

  public Optional<byte[]> asByteArray() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toByteArray());
  }

  public Optional<ByteBuffer> asByteBuffer() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.asReadOnlyByteBuffer());
  }

  /**
   * Converts the value read from cache to a UTF-8 String
   *
   * @return
   */
  public Optional<String> asStringUtf8() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }

    return Optional.ofNullable(body.toStringUtf8());
  }

  public Optional<String> asString(Charset charset) {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.toString(charset));
  }

  public Optional<InputStream> asInputStream() {
    if (result != ECacheResult.Hit) {
      return Optional.empty();
    }
    return Optional.ofNullable(body.newInput());
  }
}
