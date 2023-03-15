package momento.sdk.internal;

/** Container for low-level error information, including details from the transport layer. */
public class MomentoTransportErrorDetails {

  private final MomentoGrpcErrorDetails grpcErrorDetails;

  public MomentoTransportErrorDetails(MomentoGrpcErrorDetails grpcErrorDetails) {
    this.grpcErrorDetails = grpcErrorDetails;
  }

  public MomentoGrpcErrorDetails getGrpcErrorDetails() {
    return grpcErrorDetails;
  }
}
