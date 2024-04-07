package momento.client.example.perf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortedSetLineProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static RedisSortedSetEntry processLine(final String line) {
        try {
            JsonNode rootNode = MAPPER.readTree(line);
            String key = rootNode.get("key").asText();
            JsonNode valuesNode = rootNode.get("value");

            List<RedisSortedSetEntry.MemberScore> memberScores = new ArrayList<>();
            valuesNode
                    .fields()
                    .forEachRemaining(
                            entry -> memberScores.add(new RedisSortedSetEntry.MemberScore(entry.getKey(), entry.getValue().asDouble())));
           return new RedisSortedSetEntry(key, memberScores);
        } catch (Exception e) {
            System.err.println("Failed to parse line as JSON: ");
            return null;
        }

    }
}
