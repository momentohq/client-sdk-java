package momento.sdk.internal;

import io.grpc.Status;
import momento.sdk.exceptions.MomentoErrorMetadata;

/** Captures gRPC-level information about an error. */
public class MomentoGrpcErrorDetails {
  private final Status.Code statusCode;
  private final String details;
  private final MomentoErrorMetadata metadata;

  public MomentoGrpcErrorDetails(
      Status.Code statusCode, String details, MomentoErrorMetadata metadata) {
    this.statusCode = statusCode;
    this.details = details;
    this.metadata = metadata;
  }

  public Status.Code getStatusCode() {
    return statusCode;
  }

  public String getDetails() {
    return details;
  }

  public MomentoErrorMetadata getMetadata() {
    return metadata;
  }
}
