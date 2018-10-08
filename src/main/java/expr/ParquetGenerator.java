
package expr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.Random;

public class ParquetGenerator {
    private String SENSOR_PREFIX = "s";
    private String DEVICE_PREFIX = "d";

    private String filePath = "gen.ts_plain";
    private String schemaName = "record";
    private int sensorNum = 10;
    private int deviceNum = 10;
    private int ptNum = 1000000;
    private PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
    private Configuration configuration = new Configuration();
    private boolean usingEncoing = true;

    private Random random = new Random(System.currentTimeMillis());

    private ParquetWriter writer;
    private MessageType schema;

    public ParquetGenerator() throws IOException {

    }

    private void init() throws IOException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < deviceNum; i++) {
            Types.GroupBuilder groupBuilder = Types.buildGroup(Type.Repetition.OPTIONAL);
            for (int j = 0; j < sensorNum; j++) {
                groupBuilder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));
            }
            builder.addField((Type) groupBuilder.named(DEVICE_PREFIX + i));
        }
        schema = builder.named(schemaName);

        GroupWriteSupport.setSchema(schema, configuration);
        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        groupWriteSupport.init(configuration);
        writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
                usingEncoing, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    }

    public void write() throws IOException {
        init();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);

        for (int k = 0; k < ptNum; k++) {
            Group group = simpleGroupFactory.newGroup();
            for (int i = 0; i < deviceNum; i++) {
                group.addGroup(DEVICE_PREFIX + i);
                for (int j = 0; j < sensorNum; j++) {
                    group.getGroup(DEVICE_PREFIX + i, 0).add(SENSOR_PREFIX + j, (j + random.nextInt(100))  * 1.0);
                }
            }
            writer.write(group);
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        ParquetGenerator parquetGenerator = new ParquetGenerator();
        parquetGenerator.write();
    }
}