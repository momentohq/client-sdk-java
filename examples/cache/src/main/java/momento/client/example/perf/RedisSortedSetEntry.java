package momento.client.example.perf;

import java.util.List;
import java.util.Map;

public class RedisSortedSetEntry {

    public static class MemberScore {
        private final String member;
        private final double score;

        public MemberScore(String member, double score) {
            this.member = member;
            this.score = score;
        }

        public double getScore() {
            return score;
        }

        public String getMember() {
            return member;
        }
    }

    private final String key;
    private final List<MemberScore> memberScores;

    public RedisSortedSetEntry(String key, List<MemberScore> memberScores) {
        this.key = key;
        this.memberScores = memberScores;
    }

    public String getKey() {
        return key;
    }

    public List<MemberScore> getMemberScores() {
        return memberScores;
    }
}
