package momento.client.example.perf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTest {
  private final IngesterV2 ingester;
  private static final Logger logger = LoggerFactory.getLogger(PerformanceTest.class);

  public PerformanceTest(int numIngestionThreads) {
    String redisHost = "localhost";
    int redisPort = 6666;
    logger.info("Loaded all JSON lines in memory");
    final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(100);
    final JedisPool jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort);
    this.ingester = new IngesterV2(jedisPool, 2000, numIngestionThreads);
  }

  public void startIngestion(final String filePath, final boolean readEnabled) throws IOException  {

    long start = System.currentTimeMillis();
    final List<String> jsonLines = LeaderboardJSONLReader.parseFile(filePath);
    logger.info("Loaded " + filePath + " in " + (System.currentTimeMillis() - start) + " millis");
    logger.info("Preprocessing JSONs in memory to not hamper with ingestion; this may take a while...");

    start = System.currentTimeMillis();
    final List<RedisSortedSetEntry> sortedSetEntries =
            jsonLines.parallelStream().map(SortedSetLineProcessor::processLine).toList();
    logger.info("Preprocessed JSON entries " + sortedSetEntries.size() + " in " + (System.currentTimeMillis() - start) + " millis");

    sortedSetEntries.forEach(e -> {
      logger.info("Ingesting leaderboard " + e.getKey() + " with total elements " + e.getMemberScores().size());
      ingester.ingestSync(e, readEnabled);
    });
  }

  public static void main(String[] args) throws IOException {
    // Check if directory path is provided
    if (args.length < 3) {
      logger.info("Usage: java ingester <directoryPath> <readEnabled> <numThreads>");
      System.exit(1);
    }

    final String directoryPath = args[0];
    boolean readEnabled = Boolean.parseBoolean(args[1]);
    int numIngestionThreads = Integer.parseInt(args[2]);


    // Get the directory
    File directory = new File(directoryPath);
    if (!directory.isDirectory()) {
      logger.info(directoryPath + " is not a valid directory.");
      System.exit(2);
    }
    final PerformanceTest performanceTest = new PerformanceTest(numIngestionThreads);
    // List all files in the directory
    File[] files = directory.listFiles();
    if (files != null) {
      for (File file : files) {
        // Ensure it's a file, not a directory
        if (file.isFile()) {
          String filePath = file.getAbsolutePath();
          logger.info("Starting ingestion for " + filePath + " , readEnabled :" + readEnabled);
          performanceTest.startIngestion(filePath, readEnabled);
        }
      }
    } else {
      logger.info("The directory is empty or an I/O error occurred.");
    }
  }
}
