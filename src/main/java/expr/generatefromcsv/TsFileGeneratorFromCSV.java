package expr.generatefromcsv;

import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import com.csvreader.CsvReader;
import datagen.DataGenerator;
import datagen.GeneratorFactory;
import expr.MonitorThread;
import expr.TsFileGenerator;
import hadoop.HDFSOutputStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static cons.Constants.*;
import static cons.Constants.expReportFilePath;

public class TsFileGeneratorFromCSV {
    private TsFileWriter writer;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private static FileWriter reportWriter;

    static CsvReader reader;

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

        while(reader.readRecord()){
            String[] line = reader.getValues();

            for(int i = 1; i <= deviceNum; i++){
                TSRecord record = new TSRecord(Long.parseLong(line[0]), DEVICE_PREFIX + (i-1));
                realAllPnt++;
                for(int j = 1; j <= sensorNum; j++){
                    if(line[(i - 1) * sensorNum + j] == "") continue;
                    record.addTuple(new FloatDataPoint(SENSOR_PREFIX + (j-1), Float.parseFloat(line[(i - 1) * sensorNum + j])));
                    realAllPnt++;
                }
                writer.write(record);
            }


        }
        writer.close();
        writer = null;
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }


    private static void run(String csvPath) throws IOException, WriteProcessException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;

        reader = new CsvReader(csvPath);

        TsFileGeneratorFromCSV tsFileGeneratorFromCSV = new TsFileGeneratorFromCSV();
        if(align)
            tsFileGeneratorFromCSV.gen();


        double avgSpd = (realAllPnt) / (tsFileGeneratorFromCSV.timeConsumption / 1000.0);
        double memUsage = tsFileGeneratorFromCSV.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
        totAvgSpd += avgSpd;
        totMemUsage += memUsage;
        System.out.println(String.format("TsFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
                    avgSpd, memUsage));
        File file = new File(filePath);
        totFileSize += file.length() / (1024.0 * 1024.0);
        if (!keepFile) {
            file.delete();
        }

        System.out.println(String.format("FileName: %s; DataType: %s; Encoding: %s", filePath, dataType, encoding));
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));

        reportWriter.write(String.format("FileName: %s; DataType: %s; Encoding: %s\n", filePath, typeName, usingEncoing));
        reportWriter.write(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB\n",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
        reportWriter.write("\n");
    }



    public static void exper(String csvPath) throws IOException {
        String exInfo = csvPath.split("[.]")[0] + csvPath.split("[.]")[1];
        reportWriter.write(exInfo + ":\n");
        filePath = exInfo + ".ts";
        align = true;
        repetition = 1;
        keepFile = true;
        try {
            run(csvPath);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }

    public static void main(String[] args) throws IOException {
        String csv_path = args[0];
        expReportFilePath = "ts_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);

        CsvReader preReader = new CsvReader(csv_path);
        preReader.readRecord();
        sensorNum = preReader.getValues().length - 1;
        deviceNum = Integer.parseInt(csv_path.split("[_]")[1].split("d")[1]);
        sensorNum = Integer.parseInt(csv_path.split("[_]")[2].split("s")[1]);


        exper(csv_path);
        reportWriter.close();
    }
}
