package datagen;

public class SinFloatGenerator implements FloatGenerator {

    private float amplitude;
    private float initPhase;
    private int period;
    private float offset;
    private int limit;
    private int index;

    /**
     * The generater value will be amplitude * sin( (index + initPhase) / period * 2pi) + offset
     * @param amplitude
     * @param initPhase represented by the number of points
     * @param period  represented by the number of points
     * @param limit this object can generate up to limit points, a negative limit means positive infinity
     */
    public SinFloatGenerator(float amplitude, float initPhase, int period, int limit, float offset) {
        this.amplitude = amplitude;
        this.initPhase = initPhase;
        this.offset = offset;
        if (period <= 0)
            throw new IllegalArgumentException("period : " + period);
        this.period = period;
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
        return ((Double) (Math.sin((double) (index + initPhase) / period * 2 * Math.PI) + offset)).floatValue();
    }

    @Override
    public void reset() {
        index = 0;
    }
}
