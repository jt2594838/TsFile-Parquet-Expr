package expr;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import org.apache.parquet.schema.PrimitiveType;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.*;

public class CSVGenerator {
    private static final String COMMA = ",";
    private BufferedWriter writer;
    private DataGenerator dataGenerator;
    private MonitorThread monitorThread;
    private long timeConsumption;

    public CSVGenerator() throws IOException {

    }

    private void init() throws IOException {
        writer = new BufferedWriter(new FileWriter(filePath));
    }

    public void write() throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        for (int k = 0; k < ptNum; k++) {
            for (int i = 0; i < deviceNum; i++) {
                StringBuilder record = new StringBuilder(String.valueOf(k + 1));
                record.append(COMMA).append(DEVICE_PREFIX + "." + i);
                for (int j = 0; j < sensorNum; j++) {
                    record.append(COMMA).append(dataGenerator.next());
                }
                writer.write(record.toString());
                writer.newLine();
            }

        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            CSVGenerator generator = new CSVGenerator();
            generator.write();

            double avgSpd = (sensorNum * deviceNum * ptNum) / (generator.timeConsumption / 1000.0);
            double memUsage = generator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
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
        filePath = "expr1.csv";
        align = true;
        dataType = TSDataType.FLOAT;
        typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
        deviceNum = 50;
        sensorNum = 1000;
        repetition = 1;
        keepFile = true;
        for (int pNum : new int[]{10000}) {
            ptNum = pNum;
            run();
        }
    }
}