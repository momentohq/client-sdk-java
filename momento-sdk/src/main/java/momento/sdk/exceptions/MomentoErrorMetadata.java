package momento.sdk.exceptions;

import java.util.Optional;

/** Metadata about a failed Momento request. */
public class MomentoErrorMetadata {
  private final long requestTimeout;
  private final String cacheName;

  public MomentoErrorMetadata(long requestTimeout, String cacheName) {
    this.requestTimeout = requestTimeout;
    this.cacheName = cacheName;
  }

  /**
   * Returns the cache name of the failed request if one is present.
   *
   * @return the cache name if the request was associated with one.
   */
  public Optional<String> getCacheName() {
    return Optional.ofNullable(cacheName);
  }

  /**
   * Returns the deadline of the failed request.
   *
   * @return the timeout in seconds.
   */
  public long getRequestTimeout() {
    return requestTimeout;
  }
}
