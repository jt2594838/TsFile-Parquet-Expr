package cons;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import datagen.Waves;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.PrimitiveType;

public class Constants {

    // generation
    public static TSEncoding encoding = TSEncoding.RLE;
    public static boolean usingEncoing = true;
    public static TSDataType dataType = TSDataType.FLOAT;
    public static PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
    public static Configuration configuration = new Configuration();
    public static String schemaName = "Record";

    public static Waves wave = Waves.RANDOM;
    public static int intRange = 100;
    public static int intOffset = 0;
    public static long longRange = 100;
    public static long longOffset = 0;
    public static float floatRange = 100.0f;
    public static float floatOffset = 0.0f;
    public static double doubleRange = 100.0;
    public static double doubleOffset = 0;
    public static float squareFirstHalf = 1.0f;
    public static float squareSecondHalf = 0.0f;
    public static float amplitude = 10.0f;
    public static int phase = 0;
    public static int halfPeriod = 100;

    public static int repetition = 5;
    public static boolean keepFile = false;

    public static String SENSOR_PREFIX = "s";
    public static String DEVICE_PREFIX = "d";

    public static String filePath = "expr1.ts";
    public static int sensorNum = 10;
    public static int deviceNum = 500;
    public static long ptNum = 10000;

    // query
    public static String SEPARATOR = ".";
    public static String SESSION_NAME = "ParquetQuerier";
    public static int selectNum = 1;
}
