package momento.client.example.doc_examples;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.util.ConfigurationWatchListUtil;
import momento.sdk.PreviewStorageClient;
import momento.sdk.auth.CredentialProvider;
import momento.sdk.config.StorageConfigurations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class CheatSheet {
  public static void main(String[] args) {

    final Logger logger = LoggerFactory.getLogger(CheatSheet.class);
    LoggerContext loggerContext = ((ch.qos.logback.classic.Logger)logger).getLoggerContext();
    URL mainURL = ConfigurationWatchListUtil.getMainWatchURL(loggerContext);
    System.out.println(mainURL);
    // or even
    logger.info("Logback used '{}' as the configuration file.", mainURL);
    try (final var storageClient =
                 new PreviewStorageClient(
                         CredentialProvider.fromEnvVar("MOMENTO_API_KEY"),
                         StorageConfigurations.Laptop.latest())) {
      // ...
    }
  }
}
