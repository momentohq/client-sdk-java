package momento.client.example.doc_examples;

import momento.sdk.LeaderboardClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.LeaderboardConfigurations;

public class CheatSheet {
  public static void main(String[] args) {
    try (final LeaderboardClient leaderboardClient =
        LeaderboardClient.builder(
                CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                LeaderboardConfigurations.Laptop.latest())
            .build()) {
      // ...
    }
  }
}
