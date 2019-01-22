package expr.nodevice;

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
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.*;
import static cons.Constants.SENSOR_PREFIX;

/**
 * In this experiment, the table is designed as (time, s1, s2, ..., sn)
 * Ignoring the conception of a device
 */
public class ParquetGenerator {
    private ParquetWriter writer;
    private MessageType schema;
    private DataGenerator dataGenerator;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private static FileWriter reportWriter;

    public ParquetGenerator(){}

    private void init() throws IOException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
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

    public void gen(boolean hasNull) throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        // generate values
        if(hasNull){
            for(int k = 0; k < ptNum; k++){
                Group group = simpleGroupFactory.newGroup();
                group.add("time", (long) k + 1);
                for(int i = 0; i < sensorNum; i++)
                    group.add(SENSOR_PREFIX + i, (float) dataGenerator.next());
                writer.write(group);
            }
        }else{
            for(int k = 0; k < ptNum; k++){
//                System.out.println(k);
                Group group = simpleGroupFactory.newGroup();
                group.add("time", (long) k + 1);
                for(int i = 0; i < sensorNum; i++){
                    if(Math.random()<nullRate) continue;
                    group.add(SENSOR_PREFIX + i, (float) dataGenerator.next());
                }
                writer.write(group);
            }
        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run(boolean hasNull) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            ParquetGenerator parquetGenerator = new ParquetGenerator();
            if (align)
                parquetGenerator.gen(hasNull);
//            else
//                parquetGenerator.writeNonalign();
//            double avgSpd = (sensorNum * deviceNum * ptNum) / (parquetGenerator.timeConsumption / 1000.0);
            double avgSpd = (sensorNum * ptNum) / (parquetGenerator.timeConsumption / 1000.0);
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

        reportWriter.write(String.format("FileName: %s; DataType: %s; Encoding: %s\n", filePath, typeName, usingEncoing));
        reportWriter.write(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s\n", deviceNum, sensorNum, ptNum, wave));
        reportWriter.write(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB\n",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
        reportWriter.write("\n");
    }

    public static void exper(int lab, int x, boolean hasNull, float rate) throws IOException {
        nullRate = rate;
        String exInfo = "parquet_lab" + lab + "_x" + x;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
        filePath = "expFile\\parquet\\" + exInfo + ".parquet";
        ptNum = 100000;
        align = true;
        deviceNum = 100;
        sensorNum = x * deviceNum; // it includes all the sensors in the system
        repetition = 1;
        keepFile = true;
        try {
            run(hasNull);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }

    public static void main(String[] args) throws IOException {
        int lab_in = Integer.parseInt(args[0]),
                deviceNum_in = Integer.parseInt(args[1]) ,
                ptNum_in = Integer.parseInt(args[4]);
        boolean hasNull_in = Boolean.parseBoolean(args[2]);
        float nullRate_in = Float.parseFloat(args[3]);
        ptNum = ptNum_in;

        expReportFilePath = "report\\parque_rpt";
        new File("report").mkdir();
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(lab_in, deviceNum_in, hasNull_in, nullRate_in);




//
//        // lab1, x = 20
//        exper(1, 20, false, 0);
//
//        // lab1, x = 40
//        exper(1, 40, false, 0);
//
//
//        // lab1, x = 60
//        exper(1, 60, false, 0);
//
//
//        // lab1, x = 80
//        exper(1, 80, false, 0);
//
//
//        // lab1, x = 100
//        exper(1, 100, false, 0);
//
//
//        // lab2, x = 100, rate = 0
//        exper(2, 100, true, (float) 0);
//
//
//        // lab2, x = 100, rate = 0.2
//        exper(2, 100, true, (float) 0.2);
//
//
//        // lab2, x = 100
//        exper(2, 100, true, (float) 0.4);
//
//
//        // lab2, x = 100
//        exper(2, 100, true, (float)0.6);
//
//
//        // lab2, x = 100
//        exper(2, 100, true, (float)0.8);

        reportWriter.close();
    }

}




/*
                for(int i = 0; i < sensorNum; i++){
                    Object value = dataGenerator.next();
                    switch (dataType) {
                        case FLOAT:
                            group.add(SENSOR_PREFIX + i, (float) value);
                            break;
                        case DOUBLE:
                            group.add(SENSOR_PREFIX + i, (double) value);
                            break;
                        case INT32:
                            group.add(SENSOR_PREFIX + i, (int) value);
                            break;
                        case INT64:
                            group.add(SENSOR_PREFIX + i, (long) value);
                            break;
                    }
                }
 */
