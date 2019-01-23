package expr.nodevice;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import expr.MonitorThread;
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
    private static FileWriter reportWriter;

    public CSVGenerator() throws IOException {

    }

    private void init() throws IOException {
        writer = new BufferedWriter(new FileWriter(filePath));
    }

    public void gen(boolean hasNull) throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        if(hasNull){
            for (int k = 0; k < ptNum; k++) {
                    StringBuilder record = new StringBuilder(String.valueOf(k + 1));
                    realAllPnt++;
                    for (int j = 0; j < sensorNum; j++) {
                        if(Math.random() < nullRate) {
                            record.append(COMMA);
                            continue;
                        }
                        record.append(COMMA).append(dataGenerator.next());
                        realAllPnt++;
                    }
                    writer.write(record.toString());
                    writer.newLine();

            }
        }else{
            for (int k = 0; k < ptNum; k++) {
                    StringBuilder record = new StringBuilder(String.valueOf(k + 1));
                    realAllPnt++;
                    for (int j = 0; j < sensorNum; j++) {
                        record.append(COMMA).append(dataGenerator.next());
                        realAllPnt++;
                    }
                    writer.write(record.toString());
                    writer.newLine();
            }
        }


        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run(boolean hasNull) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;

        for (int i = 0; i < repetition; i ++) {
            CSVGenerator generator = new CSVGenerator();
            generator.gen(hasNull);

            double avgSpd = (realAllPnt) / (generator.timeConsumption / 1000.0);
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

        reportWriter.write(String.format("FileName: %s; DataType: %s; Encoding: %s\n", filePath, typeName, usingEncoing));
        reportWriter.write(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s\n", deviceNum, sensorNum, ptNum, wave));
        reportWriter.write(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB\n",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
        reportWriter.write("\n");

    }




    public static void exper(int lab, int x, boolean hasNull, float rate) throws IOException {
        nullRate = rate;
        String exInfo = "csv_lab" + lab + "_x" + x + "_rate" + rate;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
//        filePath = "expFile\\csv\\" + exInfo + ".csv";
        filePath = exInfo + ".csv";
        if(!(new File(filePath).exists())) new File(filePath).createNewFile();
//        ptNum = 100000;
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

        expReportFilePath = "csv_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(lab_in, deviceNum_in, hasNull_in, nullRate_in);

        reportWriter.close();
//
//
//
//        filePath = "expr1.csv";
//        align = true;
//        dataType = TSDataType.FLOAT;
//        typeName = PrimitiveType.PrimitiveTypeName.FLOAT;
//        deviceNum = 50;
//        sensorNum = 1000;
//        repetition = 1;
//        keepFile = true;
//        for (int pNum : new int[]{10000}) {
//            ptNum = pNum;
//            run();
//        }
    }
}