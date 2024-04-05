package momento.client.example.perf;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Lists;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTest {
  private final Ingester ingester;
  private static final Logger logger = LoggerFactory.getLogger(PerformanceTest.class);

  public PerformanceTest() {
    String redisHost = "localhost";
    int redisPort = 6666;
    final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    this.ingester = new Ingester(jedisPool, 100, 1000);
  }

  public void startIngestion(final String filePath, final boolean readEnabled) throws IOException  {

    long start = System.currentTimeMillis();
    final List<String> jsonLines = LeaderboardJSONLReader.parseFile(filePath);
    logger.info("Loaded " + filePath + " in " + (System.currentTimeMillis() - start) + " millis");

    start = System.currentTimeMillis();
    final List<RedisSortedSetEntry> sortedSetEntries =
            jsonLines.stream().map(SortedSetLineProcessor::processLine).toList();
    logger.info("Preprocessed JSON entries " + sortedSetEntries.size() + " in " + (System.currentTimeMillis() - start) + " millis");

    sortedSetEntries.forEach(e -> {
      logger.info("Ingesting " + e.getKey());
      try {
        this.ingester.ingestAsync(e, readEnabled).get();
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      } catch (ExecutionException ex) {
        throw new RuntimeException(ex);
      }
    });
  }

  public static void main(String[] args) throws IOException{
    final PerformanceTest peformanceTest = new PerformanceTest();
    if (args.length < 1) {
      logger.info("Usage: java ingester <filePath>");
      System.exit(1);
    }

    final String filePath = args[0];
    boolean readEnabled = false;
    if (args.length > 1) {
      readEnabled = Boolean.parseBoolean(args[1]);
    }

    logger.info("Starting ingestion for " + filePath + " , readEnabled :" + readEnabled);
    peformanceTest.startIngestion(filePath, readEnabled);

  }
}
