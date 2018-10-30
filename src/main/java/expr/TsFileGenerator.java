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

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

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
        // Write the data into the .csv file.
        CsvWriter csvWriter = new CsvWriter(tmpPath);
        String [] tmpRecord = new String[sensorNum + 2];
        for(int i = 0; i < ptNum; i ++) {
            Object value = dataGenerator.next();
            for(int j = 0; j < deviceNum; j ++) {
            	tmpRecord[0] = String.valueOf(i + 1);
            	tmpRecord[1] = DEVICE_PREFIX + String.valueOf(j);
                for (int k = 0; k < sensorNum; k++) {
                    switch (dataType) {
                        case DOUBLE:
                            tmpRecord[k + 2] = "DOUBLE";
                            break;
                        case FLOAT:
                            tmpRecord[k + 2] = "FLOAT";
                            break;
                        case INT32:
                            tmpRecord[k + 2] = "INT32";
                            break;
                        case INT64:
                            tmpRecord[k + 2] = "INT64";
                    }
                    tmpRecord[k + 2] += "#" + SENSOR_PREFIX + String.valueOf(k) + "#" + String.valueOf(value);
                }
                csvWriter.writeRecord(tmpRecord);
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        csvWriter.close();
        csvWriter = null;
        // Read the data from the .csv file and write it into the .ts file.
        CsvReader csvReader = new CsvReader(tmpPath);
        csvReader.setSafetySwitch(false);
        while (csvReader.readRecord()) {
        	tmpRecord = csvReader.getValues();
        	TSRecord record = new TSRecord(Long.parseLong(tmpRecord[0]), tmpRecord[1]);
        	for (int i = 0; i < sensorNum; i++) {
                DataPoint point = null;
        		String [] tmpPointList = tmpRecord[i + 2].split("#");
        		switch (tmpPointList[0]) {
	                case "DOUBLE":
	                    point = new DoubleDataPoint(tmpPointList[1], Double.parseDouble(tmpPointList[2]));
	                    break;
	                case "FLOAT":
	                    point = new FloatDataPoint(tmpPointList[1], Float.parseFloat(tmpPointList[2]));
	                    break;
	                case "INT32":
	                    point = new IntDataPoint(tmpPointList[1], Integer.parseInt(tmpPointList[2]));
	                    break;
	                case "INT64":
	                    point = new LongDataPoint(tmpPointList[1], Long.parseLong(tmpPointList[2]));
	            }
        		record.addTuple(point);
        	}
        	writer.write(record);
        }
        csvReader.close();
        csvReader = null;
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
        // Write the data into the .csv file.
        CsvWriter csvWriter = new CsvWriter(tmpPath);
        String [] tmpRecord = new String[3];
        for(int i = 0; i < ptNum; i ++) {
            Object value = dataGenerator.next();
            for(int j = 0; j < deviceNum; j ++) {
                for (int k = 0; k < sensorNum; k++) {
                	tmpRecord[0] = String.valueOf((i + 1) * sensorNum + k);
                	tmpRecord[1] = DEVICE_PREFIX + String.valueOf(j);
                	switch (dataType) {
	                    case DOUBLE:
	                        tmpRecord[2] = "DOUBLE";
	                        break;
	                    case FLOAT:
	                        tmpRecord[2] = "FLOAT";
	                        break;
	                    case INT32:
	                        tmpRecord[2] = "INT32";
	                        break;
	                    case INT64:
	                        tmpRecord[2] = "INT64";
	                }
	                tmpRecord[2] += "#" + SENSOR_PREFIX + String.valueOf(k) + "#" + String.valueOf(value);
	                csvWriter.writeRecord(tmpRecord);
                }
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                // System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        csvWriter.close();
        csvWriter = null;
        // Read the data from the .csv file and write it into the .ts file.
        CsvReader csvReader = new CsvReader(tmpPath);
        csvReader.setSafetySwitch(false);
        while (csvReader.readRecord()) {
        	tmpRecord = csvReader.getValues();
        	TSRecord record = new TSRecord(Long.parseLong(tmpRecord[0]), tmpRecord[1]);
            DataPoint point = null;
            String [] tmpPointList = tmpRecord[2].split("#");
        	switch (tmpPointList[0]) {
	        	case "DOUBLE":
	        		point = new DoubleDataPoint(tmpPointList[1], Double.parseDouble(tmpPointList[2]));
	                break;
	            case "FLOAT":
	                point = new FloatDataPoint(tmpPointList[1], Float.parseFloat(tmpPointList[2]));
	                break;
	            case "INT32":
	                point = new IntDataPoint(tmpPointList[1], Integer.parseInt(tmpPointList[2]));
	                break;
	            case "INT64":
	            	point = new LongDataPoint(tmpPointList[1], Long.parseLong(tmpPointList[2]));
        	}
        	record.addTuple(point);
        	writer.write(record);
        }
        csvReader.close();
        csvReader = null;
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
        filePath = "expr2.ts";
        align = true;
        deviceNum = 500;
        sensorNum = 10;
        repetition = 1;
        keepFile = true;
        for (int pNum : new int[]{10000}) {
            ptNum = pNum;
            run();
        }
    }
}