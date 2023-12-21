package momento.sdk.config.transport;

import static momento.sdk.ValidationUtils.ensureRequestDeadlineValid;

import java.time.Duration;
import javax.annotation.Nonnull;

/** Abstracts away the gRPC configuration tunables. */
public class GrpcConfiguration {

  private final Duration deadline;
  private final int minNumGrpcChannels;

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   */
  public GrpcConfiguration(@Nonnull Duration deadline) {
    this(deadline, 1);
  }

  /**
   * Constructs a GrpcConfiguration.
   *
   * @param deadline The maximum duration of a gRPC call.
   * @param minNumGrpcChannels The minimum number of gRPC channels to keep open at any given time.
   */
  public GrpcConfiguration(@Nonnull Duration deadline, int minNumGrpcChannels) {
    ensureRequestDeadlineValid(deadline);
    this.deadline = deadline;
    this.minNumGrpcChannels = minNumGrpcChannels;
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

  /**
   * The minimum number of gRPC channels to keep open at any given time.
   *
   * @return the minimum number of gRPC channels.
   */
  public int getMinNumGrpcChannels() {
    return minNumGrpcChannels;
  }

  /**
   * Copy constructor that updates the minimum number of gRPC channels.
   *
   * @param minNumGrpcChannels The new minimum number of gRPC channels.
   * @return The updated GrpcConfiguration.
   */
  public GrpcConfiguration withMinNumGrpcChannels(int minNumGrpcChannels) {
    return new GrpcConfiguration(deadline, minNumGrpcChannels);
  }
}
