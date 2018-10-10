package datagen;

import java.util.Random;

public class RandomDoubleGenerator implements DoubleGenerator{
    private Random random;
    private double range;
    private double offset;
    private int limit;
    private int index;

    /**
     * The generated number will be range * random[0,1] + offset.
     * @param range
     * @param offset
     * @param limit a negative index means no limit
     */
    public RandomDoubleGenerator(double range, double offset, int limit) {
        random = new Random(System.currentTimeMillis());
        this.range = range;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return limit < 0 || index < limit;
    }

    @Override
    public Double next() {
        index ++;
        if (index > limit && ! (limit < 0)) {
            throw new IndexOutOfBoundsException(String.format("%d > %d", index, limit));
        }
        return this.random.nextDouble() * range + offset;
    }

    @Override
    public void reset() {
        index = 0;
    }
}
