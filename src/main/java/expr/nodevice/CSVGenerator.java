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

    private StringBuilder getString(StringBuilder s, int i){
        for(int n = 0; n < sensorNum; n++){
            s.append(COMMA);
            if(n+1 == i) s.append(dataGenerator.next());
        }
        return s;
    }

    private StringBuilder getString(StringBuilder s, int i, int e){
        for(int n = 1; n <= i-1; n++) s.append(COMMA);
        for(int n = i; n <= e; n++)s.append(COMMA).append(dataGenerator.next());
        return s;
    }

    public void gen(boolean hasNull) throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();

        int counter = 0, line_num = 1;

        if(nullRate == (float) 0.5){
            line_num = 2;
        }else if(nullRate == (float) 0.67){
            line_num = 3;
        }else if(nullRate == (float) 0.75){
            line_num = 4;
        }else if(nullRate == (float) 0.8){
            line_num = 5;
        }
        long time = 0;

        while(counter < ptNum){

            int index = 1;
            for(int i = 0; i < line_num - 1; i++){
                StringBuilder record = new StringBuilder(String.valueOf(++time));
                writer.write(getString(record, i+1).toString());
                writer.newLine();
//                realAllPnt++;
//                realAllPnt++;
                index++;
            }
            StringBuilder record = new StringBuilder(String.valueOf(++time));
            writer.write(getString(record, index, sensorNum).toString());
            writer.newLine();
//            realAllPnt+=
            counter++;
        }


//        if(hasNull){
//            for (int k = 0; k < ptNum; k++) {
//                    StringBuilder record = new StringBuilder(String.valueOf(k + 1));
//                    realAllPnt++;
//                    for (int j = 0; j < sensorNum; j++) {
//                        if(Math.random() < nullRate) {
//                            record.append(COMMA);
//                            continue;
//                        }
//                        record.append(COMMA).append(dataGenerator.next());
//                        realAllPnt++;
//                    }
//                    writer.write(record.toString());
//                    writer.newLine();
//
//            }
//        }else{
//            for (int k = 0; k < ptNum; k++) {
//                    StringBuilder record = new StringBuilder(String.valueOf(k + 1));
//                    realAllPnt++;
//                    for (int j = 0; j < sensorNum; j++) {
//                        record.append(COMMA).append(dataGenerator.next());
//                        realAllPnt++;
//                    }
//                    writer.write(record.toString());
//                    writer.newLine();
//            }
//        }

        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run(boolean hasNull) throws IOException {
        double totFileSize = 0.0;
        realAllPnt = 0;
        CSVGenerator generator = new CSVGenerator();
        generator.gen(hasNull);
        totFileSize += new File(filePath).length() / (1024.0 * 1024.0);
        reportWriter.write(String.format("FileName: %s", filePath));
        reportWriter.write(String.format("File size: %fMB", totFileSize / repetition));
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


    /**
     *
     * @param args: lab#, senPerDev#, hasNull, nullRate, ptnum
     * @throws IOException
     */
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
    }
}