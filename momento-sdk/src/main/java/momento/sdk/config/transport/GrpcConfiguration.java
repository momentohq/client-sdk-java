package momento.sdk.config.transport;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import javax.annotation.Nonnull;

/** Abstracts away the gRPC configuration tunables. */
public class GrpcConfiguration {

  private final Duration deadline;

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public GrpcConfiguration(@Nonnull Duration deadline) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
  }

  /**
   * How long the client will wait for an RPC to complete before it is terminated with {@link
   * io.grpc.Status.Code#DEADLINE_EXCEEDED}.
   *
   * @return the deadline
   */
  public Duration getDeadline() {
    return deadline;
  }

  /**
   * Copy constructor that updates the deadline.
   *
   * @param deadline The new deadline.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withDeadline(Duration deadline) {
    return new GrpcConfiguration(deadline);
  }
}
