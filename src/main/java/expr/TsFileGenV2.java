package expr;

import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import hadoop.HDFSOutputStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.*;
import static cons.Constants.expReportFilePath;

public class TsFileGenV2 {
    private TsFileWriter writer;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;
    private static FileWriter reportWriter;

    private void initWriter() throws WriteProcessException, IOException {
        writer = new TsFileWriter(new HDFSOutputStream(filePath, true));
        for (int i = 0; i < sensorNum; i++) {
            MeasurementDescriptor descriptor = new MeasurementDescriptor(SENSOR_PREFIX + i, dataType, encoding);
            writer.addMeasurement(descriptor);
        }
    }

    private void gen() throws IOException, WriteProcessException {
        long startTime = System.currentTimeMillis();
        monitorThread = new MonitorThread();
        monitorThread.start();
        initWriter();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        int line_num = 1;
        if(nullRate == (float) 0.5) line_num = 2;
        else if(nullRate == (float) 0.67) line_num = 3;
        else if(nullRate == (float) 0.75) line_num = 4;
        else if(nullRate == (float) 0.8) line_num = 5;

        int counter = 0;
        long time = 0;
        while(counter < ptNum){
            int index = 0;
            for(int i = 0; i < line_num - 1; i++){
                TSRecord record = new TSRecord(++time, DEVICE_PREFIX + i);
                realAllPnt++;
                for(int k = 0; k < sensorNum; k++){
                    record.addTuple(new FloatDataPoint(SENSOR_PREFIX + k, (float) dataGenerator.next()));
                    realAllPnt++;
                }
                writer.write(record);
                index++;
            }

            time++;
            realAllPnt++;
            for(int i = index; i < deviceNum; i++){
                TSRecord record = new TSRecord(time, DEVICE_PREFIX + i);
                for(int j = 0; j < sensorNum; j++){
                    record.addTuple(new FloatDataPoint(SENSOR_PREFIX + j, (float) dataGenerator.next()));
                    realAllPnt++;
                }
                writer.write(record);
            }
            counter++;
        }

        writer.close();
        writer = null;
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }


    private static void run() throws IOException, WriteProcessException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;

        TsFileGenV2 generator = new TsFileGenV2();
        if (align)
            generator.gen();

        double avgSpd = (realAllPnt) / (generator.timeConsumption / 1000.0);
        double memUsage = generator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
        totAvgSpd += avgSpd;
        totMemUsage += memUsage;
        System.out.println(String.format("TsFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
                avgSpd, memUsage));
        File file = new File(filePath);
        totFileSize += file.length() / (1024.0 * 1024.0);
        if (!keepFile) file.delete();

        System.out.println(String.format("FileName: %s; DataType: %s; Encoding: %s", filePath, dataType, encoding));
        System.out.println(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s", deviceNum, sensorNum, ptNum, wave));
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));

        reportWriter.write(String.format("FileName: %s; DataType: %s; Encoding: %s\n", filePath, typeName, usingEncoing));
        reportWriter.write(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s\n", deviceNum, sensorNum, ptNum, wave));
        reportWriter.write(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB\n",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
        reportWriter.write("\n");
    }



    public static void exper(int lab, int x, float rate) throws IOException {
        nullRate = rate;
        String exInfo = "ts_lab" + lab + "_x" + x + "_rate" + rate;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
        filePath = exInfo + ".ts";

        align = true;
        deviceNum = 100;
        sensorNum = x; // it includes all the sensors in the system
        repetition = 1;
        keepFile = true;
        try {
            run();
        } catch (IOException | WriteProcessException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }


    public static void main(String[] args) throws IOException, WriteProcessException {
        int lab_in = Integer.parseInt(args[0]),
                seriesPerDevice_in = Integer.parseInt(args[1]) ,
                ptNum_in = Integer.parseInt(args[3]);
        float nullRate_in = Float.parseFloat(args[2]);
        ptNum = ptNum_in;

        expReportFilePath = "tsfile_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(lab_in, seriesPerDevice_in, nullRate_in);
        reportWriter.close();
    }
}
