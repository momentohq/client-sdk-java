package momento.client.example.perf;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class SSPerf {


  private static final AtomicLong numElementsInserted = new AtomicLong(0);
  private static final AtomicLong operationsPerSecond = new AtomicLong(0);
  private final Ingester ingester;

  public SSPerf() {
    String redisHost = "localhost";
    int redisPort = 6666;
    final JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort);
    this.ingester = new Ingester(jedisPool, 100, 100000);
  }

  public void startIngestion(final String filePath) throws IOException  {

    long start = System.currentTimeMillis();
    final List<String> jsonLines = LeaderboardJSONLReader.parseFile(filePath);
    System.out.println("Loaded " + filePath + " in " + (System.currentTimeMillis() - start) + " millis");
    start = System.currentTimeMillis();
    final List<RedisSortedSetEntry> sortedSetEntries =
            jsonLines.stream().map(SortedSetLineProcessor::processLine).toList();
    System.out.println("Loaded entries " + sortedSetEntries.size() + " in " + (System.currentTimeMillis() - start) + " millis");

    sortedSetEntries.forEach(this.ingester::ingestAsync);
  }

  public static void main(String[] args) throws IOException{
    final SSPerf ssPerf = new SSPerf();
    ssPerf.startIngestion("/Users/pratik/sandbox/leaderboard_analysis/leaderboard_500.jsonl");

  }
}
