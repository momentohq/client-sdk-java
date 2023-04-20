package momento.sdk.requests.sortedset;

import momento.sdk.messages.SortOrder;

import javax.annotation.Nullable;

/**
 * Options for fetching elements from a sorted set by score.
 */
public class FetchByScoreOptions {
    @Nullable private Double minScore;
    @Nullable private Double maxScore;
    @Nullable private SortOrder sortOrder;
    @Nullable private Integer offset;
    @Nullable private Integer count;

    public FetchByScoreOptions() {
    }

    @Nullable
    public Double getMinScore() {
        return minScore;
    }

    public void setMinScore(@Nullable Double minScore) {
        this.minScore = minScore;
    }

    @Nullable
    public Double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(@Nullable Double maxScore) {
        this.maxScore = maxScore;
    }

    @Nullable
    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(@Nullable SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    @Nullable
    public Integer getOffset() {
        return offset;
    }

    public void setOffset(@Nullable Integer offset) {
        this.offset = offset;
    }

    @Nullable
    public Integer getCount() {
        return count;
    }

    public void setCount(@Nullable Integer count) {
        this.count = count;
    }
}
