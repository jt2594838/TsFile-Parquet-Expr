package datagen;

import cons.Constants;

import static cons.Constants.*;

public class GeneratorFactor {

    public static GeneratorFactor INSTANCE = new GeneratorFactor();

    public DataGenerator getGenerator() {
        DataGenerator dataGenerator = null;
        switch (Constants.dataType) {
            case INT32:
                dataGenerator = new RandomIntGenerator(intRange, intOffset, -1);
                break;
            case INT64:
                dataGenerator = new RandomLongGenerator(longRange, longOffset, -1);
                break;
            case DOUBLE:
                dataGenerator = new RandomDoubleGenerator(doubleRange, doubleOffset, -1);
                break;
            case FLOAT:
                switch (wave) {
                    case RANDOM:
                        dataGenerator = new RandomFLoatGenerator(floatRange, floatOffset, -1);
                        break;
                    case SIN:
                        dataGenerator = new SinFloatGenerator(amplitude, phase, halfPeriod * 2,-1, floatOffset);
                        break;
                    case SQUARE:
                        dataGenerator = new SquareWaveFloatGenerator(squareFirstHalf, squareSecondHalf, halfPeriod, -1);
                }
                break;
        }
        return dataGenerator;
    }
}
