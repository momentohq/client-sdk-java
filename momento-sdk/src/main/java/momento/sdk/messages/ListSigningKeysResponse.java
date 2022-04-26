package momento.sdk.messages;

import java.util.List;
import java.util.Optional;
import momento.sdk.SimpleCacheClient;

/** Response object for list of signing keys. */
public final class ListSigningKeysResponse {

  private final List<SigningKey> signingKeys;
  private final Optional<String> nextPageToken;

  public ListSigningKeysResponse(List<SigningKey> signingKeys, Optional<String> nextPageToken) {
    this.signingKeys = signingKeys;
    this.nextPageToken = nextPageToken;
  }

  public List<SigningKey> signingKeys() {
    return signingKeys;
  }

  /**
   * Next Page Token returned by Simple Cache Service along with the list of signing keys.
   *
   * <p>If nextPageToken().isPresent(), then this token must be provided in the next call to
   * continue paginating through the list. This is done by setting the value in {@link
   * SimpleCacheClient#listSigningKeys(Optional)}
   *
   * <p>When not present, there are no more signingKeys to return.
   */
  public Optional<String> nextPageToken() {
    return nextPageToken;
  }
}
