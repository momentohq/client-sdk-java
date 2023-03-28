package momento.sdk;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import momento.sdk.messages.CacheListConcatenateBackResponse;
import momento.sdk.requests.CollectionTtl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ListTest extends BaseTestClass {
  private static final Duration DEFAULT_TTL_SECONDS = Duration.ofSeconds(60);
  private CacheClient target;
  private String cacheName;

  private final List<byte[]> values = Arrays.asList("val1".getBytes(), "val2".getBytes());

  private final String listName = "listName";

  @BeforeEach
  void setup() {
    target = CacheClient.builder(System.getenv("TEST_AUTH_TOKEN"), DEFAULT_TTL_SECONDS).build();
    cacheName = System.getenv("TEST_CACHE_NAME");
    target.createCache(cacheName);
  }

  @AfterEach
  void teardown() {
    target.deleteCache(cacheName);
    target.close();
  }

  @Test
  public void shouldAddStringValueToBackOfListWhenListConcatenateBack() {
    List<String> oldValues = Arrays.asList("val1", "val2", "val3");
    List<String> newValues = Arrays.asList("val4", "val5", "val6");
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, oldValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size());

    cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, newValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size() + newValues.size());
  }

  @Test
  public void shouldAddByteValueToBackOfListWhenListConcatenateBack() {
    final String listName = "listName";
    final List<byte[]> oldValues = Arrays.asList("val1".getBytes(), "val2".getBytes());
    final List<byte[]> newValues = Arrays.asList("val3".getBytes(), "val4".getBytes());
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, oldValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size());

    cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(
                cacheName, listName, newValues, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Success.class);
    assertThat(
            ((CacheListConcatenateBackResponse.Success) cacheListConcatenateBackResponse)
                .getListLength())
        .isEqualTo(oldValues.size() + newValues.size());
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullCacheName() {
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(null, listName, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Error.class);
  }

  @Test
  public void shouldFailListConcatenateBackWhenNullListName() {
    CacheListConcatenateBackResponse cacheListConcatenateBackResponse =
        target
            .listConcatenateBack(cacheName, null, values, CollectionTtl.of(DEFAULT_TTL_SECONDS), 0)
            .join();

    assertThat(cacheListConcatenateBackResponse)
        .isInstanceOf(CacheListConcatenateBackResponse.Error.class);
  }
}
