package momento.sdk.internal;

/** Container for low-level error information, including details from the transport layer. */
public class MomentoTransportErrorDetails {

  private final MomentoGrpcErrorDetails grpcErrorDetails;

  /**
   * Constructs a MomentoTransportErrorDetails.
   *
   * @param grpcErrorDetails gRPC information for the failed call.
   */
  public MomentoTransportErrorDetails(MomentoGrpcErrorDetails grpcErrorDetails) {
    this.grpcErrorDetails = grpcErrorDetails;
  }

  /**
   * Gets the gRPC information for the failed call.
   *
   * @return the gRPC details.
   */
  public MomentoGrpcErrorDetails getGrpcErrorDetails() {
    return grpcErrorDetails;
  }
}
