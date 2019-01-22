package expr.consDevice;

import datagen.DataGenerator;
import datagen.GeneratorFactory;
import expr.MonitorThread;
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

public class ParquetGeneratorV2 {

    private ParquetWriter writer;
    private MessageType schema;
    private DataGenerator dataGenerator;
    private MonitorThread monitorThread;
    private long timeConsumption;

    public ParquetGeneratorV2() throws IOException {

    }

    private void init() throws IOException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "device"));
        for (int j = 0; j < sensorNum; j++) {
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));
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
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        for (int k = 0; k < ptNum; k++) {
            for (int i = 0; i < deviceNum; i++) {
                Group group = simpleGroupFactory.newGroup();
                group.add("time", (long) k + 1);
                group.add("device", DEVICE_PREFIX + i);
                for (int j = 0; j < sensorNum; j++) {
                    Object value = dataGenerator.next();
                    switch (dataType) {
                        case FLOAT:
                            group.add(SENSOR_PREFIX + j, (float) value);
                            break;
                        case DOUBLE:
                            group.add(SENSOR_PREFIX + j, (double) value);
                            break;
                        case INT32:
                            group.add(SENSOR_PREFIX + j, (int) value);
                            break;
                        case INT64:
                            group.add(SENSOR_PREFIX + j, (long) value);
                            break;
                    }
                }
                writer.write(group);
            }

        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    public void writeNonalign() throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        for (int k = 0; k < ptNum; k++) {
            Object value = dataGenerator.next();
            for (int i = 0; i < deviceNum; i++) {
                for (int j = 0; j < sensorNum; j++) {
                    Group group = simpleGroupFactory.newGroup();
                    group.add("time", (long) ((k + 1) * sensorNum + j));
                    group.addGroup(DEVICE_PREFIX + i);
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
                    writer.write(group);
                }
            }
        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            ParquetGeneratorV2 parquetGenerator = new ParquetGeneratorV2();
            if (align)
                parquetGenerator.write();
            else
                parquetGenerator.writeNonalign();
            double avgSpd = (sensorNum * deviceNum * ptNum) / (parquetGenerator.timeConsumption / 1000.0);
            double memUsage = parquetGenerator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
            totAvgSpd += avgSpd;
            totMemUsage += memUsage;
            System.out.println(String.format("ParquetFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
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

    public static void main(String[] args) throws IOException {
        filePath = "expr2_cd.parquet";
        align = true;
        deviceNum = 100;
        sensorNum = 100;
        repetition = 1;
        keepFile = true;
        for (int pNum : new int[]{1000}) {
            ptNum = pNum;
            run();
        }
    }
}