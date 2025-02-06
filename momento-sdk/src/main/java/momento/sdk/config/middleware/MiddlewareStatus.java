package momento.sdk.config.middleware;

import io.grpc.Status;

public class MiddlewareStatus {
  private final Status grpcStatus;

  public MiddlewareStatus(Status status) {
    this.grpcStatus = status;
  }

  public Status.Code getCode() {
    return grpcStatus.getCode();
  }

  public Status getGrpcStatus() {
    return grpcStatus;
  }
}
