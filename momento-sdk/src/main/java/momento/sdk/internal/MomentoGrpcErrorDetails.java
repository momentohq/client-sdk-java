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

  /**
   * Constructs a MomentoGrpcErrorDetails.
   *
   * @param statusCode The gRPC error code.
   * @param details A detailed error message.
   */
  public MomentoGrpcErrorDetails(Status.Code statusCode, String details) {
    this(statusCode, details, null);
  }

  /**
   * Constructs a MomentoGrpcErrorDetails.
   *
   * @param statusCode The gRPC error code.
   * @param details A detailed error message.
   * @param metadata Metadata about the gRPC call.
   */
  public MomentoGrpcErrorDetails(Status.Code statusCode, String details, Metadata metadata) {
    this.statusCode = statusCode;
    this.details = details;
    this.metadata = metadata;
  }

  /**
   * Returns the gRPC error code.
   *
   * @return The error code.
   */
  public Status.Code getStatusCode() {
    return statusCode;
  }

  /**
   * Returns the detailed error message;
   *
   * @return The error message.
   */
  public String getDetails() {
    return details;
  }

  /**
   * Returns the gRPC metadata if present.
   *
   * @return The metadata.
   */
  public Optional<Metadata> getMetadata() {
    return Optional.ofNullable(metadata);
  }

  /**
   * Returns the cache name from the metadata if present.
   *
   * @return The cache name.
   */
  public Optional<String> getCacheName() {
    return Optional.ofNullable(metadata)
        .map(m -> m.get(Metadata.Key.of("cache", ASCII_STRING_MARSHALLER)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MomentoGrpcErrorDetails{");
    sb.append("statusCode=").append(statusCode);
    sb.append(", details=\"").append(details).append('\"');
    sb.append(", metadata=").append(metadata);
    sb.append('}');
    return sb.toString();
  }
}
