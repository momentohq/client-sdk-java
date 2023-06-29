package momento.client.example;

import org.slf4j.Logger;

public abstract class ExampleUtils {

  public static void logStartBanner(Logger logger) {
    logger.info("******************************************************************");
    logger.info("Example Start");
    logger.info("******************************************************************");
  }

  public static void logEndBanner(Logger logger) {
    logger.info("******************************************************************");
    logger.info("Example End");
    logger.info("******************************************************************");
  }
}
