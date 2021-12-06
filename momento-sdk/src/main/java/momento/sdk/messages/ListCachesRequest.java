package momento.sdk.messages;

import java.util.Optional;

/**
 * Request object for listing caches. Includes an optional next page token, to handle large
 * paginated lists.
 */
public final class ListCachesRequest {

  private final Optional<String> nextPageToken;

  ListCachesRequest(Optional<String> nextPageToken) {
    this.nextPageToken = nextPageToken;
  }

  public Optional<String> nextPageToken() {
    return nextPageToken;
  }

  public static ListCachesRequestBuilder builder() {
    return new ListCachesRequestBuilder();
  }

  public static class ListCachesRequestBuilder {

    private Optional<String> nextPageToken = Optional.empty();

    public ListCachesRequestBuilder nextPageToken(Optional<String> nextPageToken) {
      Optional<String> nextPageTokenOpt = nextPageToken == null ? Optional.empty() : nextPageToken;
      this.nextPageToken = nextPageTokenOpt;
      return this;
    }

    public ListCachesRequest build() {
      return new ListCachesRequest(nextPageToken);
    }
  }
}
