
package expr;

import datagen.DataGenerator;
import datagen.GeneratorFactor;
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

import java.io.File;
import java.io.IOException;

import static cons.Constants.*;

public class ParquetGenerator {

    private ParquetWriter writer;
    private MessageType schema;
    private DataGenerator dataGenerator;
    private MonitorThread monitorThread;
    private long timeConsumption;

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
        new File(filePath).delete();
        writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
                usingEncoing, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    }

    public void write() throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        dataGenerator = GeneratorFactor.INSTANCE.getGenerator();

        for (int k = 0; k < ptNum; k++) {
            Object value = dataGenerator.next();
            Group group = simpleGroupFactory.newGroup();
            for (int i = 0; i < deviceNum; i++) {
                group.addGroup(DEVICE_PREFIX + i);
                for (int j = 0; j < sensorNum; j++) {
                    switch (dataType) {
                        case FLOAT:
                            group.getGroup(DEVICE_PREFIX + i, 0).add(SENSOR_PREFIX + j, (float) value);
                            break;
                        case DOUBLE:
                            group.getGroup(DEVICE_PREFIX + i, 0).add(SENSOR_PREFIX + j, (double) value);
                            break;
                        case INT32:
                            group.getGroup(DEVICE_PREFIX + i, 0).add(SENSOR_PREFIX + j, (int) value);
                            break;
                        case INT64:
                            group.getGroup(DEVICE_PREFIX + i, 0).add(SENSOR_PREFIX + j, (long) value);
                            break;
                    }

                }
            }
            writer.write(group);
        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws IOException {

        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            ParquetGenerator parquetGenerator = new ParquetGenerator();
            parquetGenerator.write();
            double avgSpd = (sensorNum * deviceNum * ptNum) / (parquetGenerator.timeConsumption / 1000.0);
            double memUsage = parquetGenerator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
            totAvgSpd += avgSpd;
            totMemUsage += memUsage;
            System.out.println(String.format("TsFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
                    avgSpd, memUsage));
            File file = new File(filePath);
            totFileSize += file.length() / (1024.0 * 1024.0);
            if (!keepFile) {
                file.delete();
            }
        }
        System.out.println(String.format("FileName: %s; DataType: %s; Encoding: %s", filePath, typeName, usingEncoing));
        System.out.println(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s", deviceNum, sensorNum, ptNum, wave));
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
    }
}