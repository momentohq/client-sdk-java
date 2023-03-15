package momento.sdk.internal;

import io.grpc.Status;
import java.util.Map;

/** Captures gRPC-level information about an error. */
public class MomentoGrpcErrorDetails {
  private final Status.Code statusCode;
  private final String details;
  private final Map<String, String> metadata;

  public MomentoGrpcErrorDetails(
      Status.Code statusCode, String details, Map<String, String> metadata) {
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

  public Map<String, String> getMetadata() {
    return metadata;
  }
}
