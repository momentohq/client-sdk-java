package momento.sdk.messages;

import com.google.protobuf.ByteString;
import grpc.cache_client.ECacheResult;
import java.nio.ByteBuffer;

public final class CacheGetResponse extends BaseResponse {
  private final ByteString body;
  private final ECacheResult result;

  public CacheGetResponse(ECacheResult result, ByteString body) {
    this.body = body;
    this.result = result;
  }

  public MomentoCacheResult getResult() {
    return this.resultMapper(this.result);
  }

  public byte[] asByteArray() {
    return body.toByteArray();
  }

  public ByteBuffer asByteBuffer() {
    return body.asReadOnlyByteBuffer();
  }

  /**
   * Converts the value read from cache to a UTF-8 String
   *
   * @return
   */
  public String asStringUtf8() {
    return body.toStringUtf8();
  }
}
