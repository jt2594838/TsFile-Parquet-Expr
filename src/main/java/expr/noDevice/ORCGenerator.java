package expr.noDevice;

import datagen.DataGenerator;
import datagen.GeneratorFactory;
import expr.MonitorThread;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import static cons.Constants.*;


/**
 * In this experiment, the table is designed as (time, s1, s2, ..., sn)
 * Ignoring the conception of a device
 */
public class ORCGenerator {

    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;
    private Writer writer;
    private TypeDescription schema;

    private static FileWriter reportWriter;

    public ORCGenerator() {
    }

    /**
     * To generate a string, which describe the structure of the table (schema).
     * For example, "struct<time:bigint,s1:float,s2:float>"
     *
     * @return
     */
    private static String genStringSchema() {
        String s = "struct<time:timestamp";
        for (int i = 0; i < sensorNum; i++) {
            s += ("," + SENSOR_PREFIX + i + ":" + dataType.toString());
        }
        s += ">";
        return s;
    }

    /**
     * Split a schema string, get a string of array, which describe the datatypes of all attributes
     * for example, ["bigdata", "int", "int"]
     *
     * @param s, string of the schema
     * @return an array of string
     */
    private static String[] splitStrSchema(String s) {
        int t = -1;

        for (int i = s.length() - 1; i >= 0; i--) { // to get the last index of '>' in s
            if (s.toCharArray()[i] == '>') {
                t = i;
                break;
            }
        }
        String ss = s.substring(s.indexOf("<") + 1, t);
        String[] a = ss.split(",");
        String[] ans = new String[a.length];
        for (int i = 0; i < ans.length; i++)
            ans[i] = a[i].split(":")[1];
        return ans;
    }

    /**
     * Initialize writer object
     *
     * @throws IOException
     */
    private void initWriter() throws IOException {
        schema = TypeDescription.fromString(genStringSchema());
        new File(filePath).delete(); // delete file of the given name if already exists
        writer = OrcFile.createWriter(new Path(filePath),
                                      OrcFile.writerOptions(configuration)
                                              .setSchema(schema)
                                              .compress(CompressionKind.SNAPPY)
                                              .version(OrcFile.Version.V_0_12));
    }

    /**
     * Generate data and write data into database
     *
     * @throws IOException
     */
    private void gen(boolean hasNull) throws IOException {
        // set time
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();

        // preparing
        initWriter();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        VectorizedRowBatch batch = schema.createRowBatch();
        String[] a = splitStrSchema(genStringSchema());

        // generate values
        int row = 0;
        if(hasNull){
            for(int k = 0; k < ptNum; k++){
                LongColumnVector timeCol = (LongColumnVector) batch.cols[0];
                timeCol.vector[row] = (k + 1);
                for (int i = 0; i < sensorNum; i++) {
                    if(Math.random()  < nullRate) continue;
                    DoubleColumnVector s = (DoubleColumnVector) batch.cols[i + 1];
                    s.vector[row] = (float) dataGenerator.next();
                }
                row = batch.size++;
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
        }else{
            for (int k = 0; k < ptNum; k++) {
                TimestampColumnVector timeCol = (TimestampColumnVector) batch.cols[0];
                timeCol.time[row] = (k + 1);
                for (int i = 0; i < sensorNum; i++) {
                    DoubleColumnVector s = (DoubleColumnVector) batch.cols[i + 1];
                    s.vector[row] = (float) dataGenerator.next();
                }
                row = batch.size++; // update row index
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }


    public static void run(boolean hasNull) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i++) {
            ORCGenerator orcGenerator = new ORCGenerator();
            if (align)
                orcGenerator.gen(hasNull);
//            else
//                orcGenerator.genNonalign();
//            double avgSpd = (sensorNum * deviceNum * ptNum) / (orcGenerator.timeConsumption / 1000.0);
            double avgSpd = (sensorNum * ptNum) / (orcGenerator.timeConsumption / 1000.0);
            double memUsage = orcGenerator.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
            totAvgSpd += avgSpd;
            totMemUsage += memUsage;
            System.out.println(String.format("ORCFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
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
        String exInfo = "orc_lab" + lab + "_x" + x;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
        filePath = "expFile\\orc\\" + exInfo + ".orc";
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

    /**
     *
     * @param args, # lab, # sensor per device, hasNull, rate, ptNum
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {

        int _lab = Integer.parseInt(args[0]),
                _x = Integer.parseInt(args[1]) ,
                _ptNum = Integer.parseInt(args[4]);
        boolean _hasNull = Boolean.parseBoolean(args[2]);
        float _rate = Float.parseFloat(args[3]);

        ptNum = _ptNum;


        expReportFilePath = "report\\orc_rpt";
        new File("report").mkdir();
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(_lab, _x, _hasNull, _rate);
//
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
