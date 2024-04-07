package momento.client.example.perf;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class ProcessedMembers {
    public static class MemberBatch {
        private final List<RedisSortedSetEntry.MemberScore> memberScoresBatch;
        public MemberBatch(final List<RedisSortedSetEntry.MemberScore> entries) {
            this.memberScoresBatch = entries;
        }

        public List<RedisSortedSetEntry.MemberScore> getMemberScoresBatch() {
            return memberScoresBatch;
        }
    }

    private final Queue<MemberBatch> queue = new LinkedBlockingQueue<>();

    public void add(MemberBatch member) {
        this.queue.add(member);
    }

    public MemberBatch remove() {
        return this.queue.poll();
    }

    public boolean hasMembers() {
        return !this.queue.isEmpty();
    }
}
