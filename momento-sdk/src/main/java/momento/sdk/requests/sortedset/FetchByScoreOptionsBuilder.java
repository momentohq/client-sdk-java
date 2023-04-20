package momento.sdk.requests.sortedset;

import momento.sdk.messages.SortOrder;

import javax.annotation.Nonnull;

/**
 * A builder for {@link FetchByScoreOptions}.
 */
public class FetchByScoreOptionsBuilder {
    private FetchByScoreOptions options = new FetchByScoreOptions();

    public FetchByScoreOptionsBuilder() {
    }

    public FetchByScoreOptionsBuilder minScore(double minScore) {
        options.setMinScore(minScore);
        return this;
    }

    public FetchByScoreOptionsBuilder maxScore(double maxScore) {
        options.setMaxScore(maxScore);
        return this;
    }

    public FetchByScoreOptionsBuilder sortOrder(@Nonnull SortOrder sortOrder) {
        options.setSortOrder(sortOrder);
        return this;
    }

    public FetchByScoreOptions build() {
        return options;
    }
}
