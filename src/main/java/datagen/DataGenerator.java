package datagen;

import java.util.Iterator;

public interface DataGenerator<T> extends Iterator<T> {
    void reset();
}
