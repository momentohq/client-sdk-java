package momento.client.example.perf;


import java.util.Map;

public class RedisSortedSetEntry {
    private String key;
    private Map<String, Double> memberScores;

    public RedisSortedSetEntry(String key, Map<String, Double> memberScores) {
        this.key = key;
        this.memberScores = memberScores;
    }

    public String getKey() {
        return key;
    }

    public Map<String, Double> getMemberScores() {
        return memberScores;
    }
}
