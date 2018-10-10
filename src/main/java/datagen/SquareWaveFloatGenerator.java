package datagen;

public class SquareWaveFloatGenerator implements FloatGenerator {

    private float firstHalf;
    private float secondHalf;
    private int halfPeriod;
    private int limit;
    private int index;

    /**
     *
     * @param firstHalf  the value in the first half of the halfPeriod
     * @param secondHalf the value in the second half of the halfPeriod
     * @param halfPeriod how many points does a HALF period contains, must be positive
     * @param limit this object can generate up to limit points, a negative limit means positive infinity
     */
    public SquareWaveFloatGenerator(float firstHalf, float secondHalf, int halfPeriod, int limit) {
        this.firstHalf = firstHalf;
        this.secondHalf = secondHalf;
        if (halfPeriod <= 0)
            throw new IllegalArgumentException("Period : " + halfPeriod);
        this.halfPeriod = halfPeriod;
        this.limit = limit;
    }

    @Override
    public boolean hasNext() {
        return limit < 0 || index < limit;
    }

    @Override
    public Float next() {
        index ++;
        if (index > limit && !(limit < 0))
            throw new IndexOutOfBoundsException(String.format("%d > %d", index, limit));
        int phase = index % (2 * halfPeriod);
        return phase < halfPeriod ? firstHalf : secondHalf;
    }

    @Override
    public void reset() {
        index = 0;
    }
}
