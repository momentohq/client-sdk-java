package momento.sdk.config.middleware;

import io.grpc.Metadata;

public class MiddlewareMetadata {
  private final Metadata grpcMetadata;

  public MiddlewareMetadata(Metadata metadata) {
    this.grpcMetadata = metadata;
  }

  public Metadata getGrpcMetadata() {
    return grpcMetadata;
  }
}
