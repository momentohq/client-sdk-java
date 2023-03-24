package momento.sdk.internal;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import io.grpc.Metadata;
import io.grpc.Status;
import java.util.Optional;

/** Captures gRPC-level information about an error. */
public class MomentoGrpcErrorDetails {
  private final Status.Code statusCode;
  private final String details;
  private final Metadata metadata;

  public MomentoGrpcErrorDetails(Status.Code statusCode, String details) {
    this(statusCode, details, null);
  }

  public MomentoGrpcErrorDetails(Status.Code statusCode, String details, Metadata metadata) {
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

  public Optional<Metadata> getMetadata() {
    return Optional.ofNullable(metadata);
  }

  public Optional<String> getCacheName() {
    return Optional.ofNullable(metadata)
        .map(m -> m.get(Metadata.Key.of("cache", ASCII_STRING_MARSHALLER)));
  }
}
