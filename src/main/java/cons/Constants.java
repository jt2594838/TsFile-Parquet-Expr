package cons;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
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
    public static int intRange = 10000;
    public static int intOffset = 0;
    public static long longRange = 10000;
    public static long longOffset = 0;
    public static float floatRange = 10000.0f;
    public static float floatOffset = 0.0f;
    public static double doubleRange = 100.0;
    public static double doubleOffset = 0;
    public static float floatSquareFirstHalf = 100.0f;
    public static float floatSquareSecondHalf = 0.0f;
    public static int intSquareFirstHalf = 0;
    public static int intSquareSecondHalf = 100;
    public static long longSquareFirstHalf = 0;
    public static long longSquareSecondHalf = 100;
    public static float amplitude = 10.0f;
    public static int phase = 0;
    public static int halfPeriod = 1000;

    public static float nullRate = 0;

    public static int repetition = 1;
    public static boolean keepFile = true;

    public static String SENSOR_PREFIX = "s";
    public static String DEVICE_PREFIX = "d";

    public static String filePath = "";
    public static String expReportFilePath = "";

    public static int sensorNum = 10;
    public static int deviceNum = 1000;
    public static long ptNum = 20000;

    // query
    public static String SEPARATOR = ".";
    public static String SESSION_NAME = "ParquetSparkQuerier";
    public static boolean useFilter = true;
    public static int selectNum = 1;
    public static double selectRate = 0.1;

    public static boolean align = true;

    static {
        TSFileConfig tsFileConfig = TSFileDescriptor.getInstance().getConfig();
        tsFileConfig.compressor = "SNAPPY";
        tsFileConfig.pageSizeInByte = 1 * 1024 * 1024;
        tsFileConfig.groupSizeInByte = 1 * 128 * 1024 * 1024;
    }

    public boolean isNull(float rate){
        return Math.random()  < rate;
    }
}
