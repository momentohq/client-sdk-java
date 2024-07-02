package momento.sdk.utils;

import java.util.Optional;
import momento.sdk.exceptions.ClientSdkException;

/**
 * A wrapper around {@link Optional} that provides custom error handling for empty optionals.
 *
 * @param <T> the type of the optional value.
 */
public class MomentoOptional<T> {
  /** The optional value. */
  private final Optional<T> optional;
  /** The exception message to throw when the optional is empty. */
  private final String onEmptyExceptionMessage;

  /**
   * Constructs a new optional.
   *
   * @param optional the optional value.
   */
  private MomentoOptional(Optional<T> optional) {
    this(optional, "");
  }

  /**
   * Constructs a new optional.
   *
   * @param optional the optional value.
   * @param onEmptyExceptionMessage the exception message to throw when the optional is empty on
   *     access.
   */
  private MomentoOptional(Optional<T> optional, String onEmptyExceptionMessage) {
    this.optional = optional;
    this.onEmptyExceptionMessage = onEmptyExceptionMessage;
  }

  /**
   * Creates a new optional with the given value.
   *
   * @param value the value.
   * @return the optional.
   * @param <T> the type of the value.
   */
  public static <T> MomentoOptional<T> of(T value) {
    return new MomentoOptional<>(Optional.of(value));
  }

  /**
   * Creates a new optional with the given value.
   *
   * @param onEmptyExceptionMessage the exception message to throw when the optional is empty on
   * @return the optional.
   * @param <T> the type of the value.
   */
  public static <T> MomentoOptional<T> empty(String onEmptyExceptionMessage) {
    return new MomentoOptional<>(Optional.empty(), onEmptyExceptionMessage);
  }

  /**
   * Gets the value of the optional.
   *
   * @return the value.
   */
  public T get() {
    return optional.get();
  }

  /**
   * Gets the value of the optional or throws an exception if the optional is empty.
   *
   * @return the value.
   */
  public T orElseThrow() {
    return optional.orElseThrow(() -> new ClientSdkException(onEmptyExceptionMessage));
  }

  /**
   * Converts the optional to a standard {@link Optional}.
   *
   * @return the optional.
   */
  public Optional<T> toOptional() {
    return optional;
  }

  /**
   * Checks if the optional is present.
   *
   * @return true if the optional is present, false otherwise.
   */
  public boolean isPresent() {
    return optional.isPresent();
  }

  /**
   * Checks if the optional is empty.
   *
   * @return true if the optional is empty, false otherwise.
   */
  public boolean isEmpty() {
    return !isPresent();
  }

  /**
   * Returns a string representation of the optional.
   *
   * @return the string representation.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("MomentoOptional{");
    sb.append("optional=");
    sb.append(optional);
    sb.append('}');
    return sb.toString();
  }
}
