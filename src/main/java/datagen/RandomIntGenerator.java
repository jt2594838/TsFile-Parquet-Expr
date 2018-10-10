package datagen;

import java.util.Random;

public class RandomIntGenerator implements IntGenerator {

    private Random random;
    private int range;
    private int offset;
    private int limit;
    private int index;

    public RandomIntGenerator(int range, int offset, int limit) {
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
    public Integer next() {
        index ++;
        if (index > limit && ! (limit < 0)) {
            throw new IndexOutOfBoundsException(String.format("%d > %d", index, limit));
        }
        return this.random.nextInt(range) + offset;
    }

    @Override
    public void reset() {
        index = 0;
    }
}
