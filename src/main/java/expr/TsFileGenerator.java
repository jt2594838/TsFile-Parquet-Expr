package expr;

import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DoubleDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.LongDataPoint;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import hadoop.HDFSOutputStream;

import java.io.File;
import java.io.IOException;

import static cons.Constants.*;

public class TsFileGenerator {

    private TsFileWriter writer;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;

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
        for(int i = 0; i < ptNum; i ++) {
            Object value = dataGenerator.next();
            for(int j = 0; j < deviceNum; j ++) {
                TSRecord record = new TSRecord(i + 1, DEVICE_PREFIX + j);
                for (int k = 0; k < sensorNum; k++) {
                    DataPoint point = null;
                    switch (dataType) {
                        case DOUBLE:
                            point = new DoubleDataPoint(SENSOR_PREFIX + k, (double) value);
                            break;
                        case FLOAT:
                            point = new FloatDataPoint(SENSOR_PREFIX + k, (float) value);
                            break;
                        case INT32:
                            point = new IntDataPoint(SENSOR_PREFIX + k, (int) value);
                            break;
                        case INT64:
                            point = new LongDataPoint(SENSOR_PREFIX + k, (long) value);
                    }
                    record.addTuple(point);
                }
                writer.write(record);
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        writer.close();
        writer = null;
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private void genNonalign() throws IOException, WriteProcessException {
        long startTime = System.currentTimeMillis();
        monitorThread = new MonitorThread();
        monitorThread.start();
        initWriter();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        for(int i = 0; i < ptNum; i ++) {
            for(int j = 0; j < deviceNum; j ++) {
                for (int k = 0; k < sensorNum; k++) {
                    Object value = dataGenerator.next();
                    TSRecord record = new TSRecord((long) ((i + 1) * sensorNum + k), DEVICE_PREFIX + j);
                    DataPoint point = null;
                    switch (dataType) {
                        case DOUBLE:
                            point = new DoubleDataPoint(SENSOR_PREFIX + k, (double) value);
                            break;
                        case FLOAT:
                            point = new FloatDataPoint(SENSOR_PREFIX + k, (float) value);
                            break;
                        case INT32:
                            point = new IntDataPoint(SENSOR_PREFIX + k, (int) value);
                            break;
                        case INT64:
                            point = new LongDataPoint(SENSOR_PREFIX + k, (long) value);
                    }
                    record.addTuple(point);
                    writer.write(record);
                }
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        writer.close();
        writer = null;
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException, WriteProcessException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i ++) {
            TsFileGenerator generator = new TsFileGenerator();
            if (align)
                generator.gen();
            else
                generator.genNonalign();
            double avgSpd = (sensorNum * deviceNum * ptNum) / (generator.timeConsumption / 1000.0);
            double memUsage = generator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
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
        System.out.println(String.format("FileName: %s; DataType: %s; Encoding: %s", filePath, dataType, encoding));
        System.out.println(String.format("DeviceNum: %d; SensorNum: %d; PtPerCol: %d; Wave: %s", deviceNum, sensorNum, ptNum, wave));
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
    }

    public static void main(String[] args) throws IOException, WriteProcessException {
        filePath = "expr2-2.ts";
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