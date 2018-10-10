package datagen;

import java.util.Random;

public class RandomLongGenerator implements LongGenerator {
    private Random random;
    private long range;
    private long offset;
    private int limit;
    private int index;

    public RandomLongGenerator(long range, long offset, int limit) {
        this.random = new Random(System.currentTimeMillis());
        this.range = range;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return limit < 0 || index < limit;
    }

    @Override
    public Long next() {
        index ++;
        if (index > limit && ! (limit < 0)) {
            throw new IndexOutOfBoundsException(String.format("%d > %d", index, limit));
        }
        return this.random.nextLong() % range  + offset;
    }

    @Override
    public void reset() {
        index = 0;
    }
}
