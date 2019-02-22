package expr.nodevice;

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

import static cons.Constants.*;


/**
 * In this experiment, the table is designed as (time, s1, s2, ..., sn)
 * Ignoring the conception of a device
 */
public class ORCGeneratorV2 {

    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;
    private Writer writer;
    private TypeDescription schema;
    private VectorizedRowBatch batch;

    private static FileWriter reportWriter;

    public ORCGeneratorV2() {
    }

    /**
     * To generate a string, which describe the structure of the table (schema).
     * For example, "struct<time:bigint,s1:float,s2:float>"
     *
     * @return
     */
    private static String genStringSchema() {
        String s = "struct<time:bigint";
        for (int i = 0; i < sensorNum; i++) {
            s += ("," + SENSOR_PREFIX + i + ":" + dataType.toString());
        }
        s += ">";
        return s;
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
    private void gen(int lineNumber) throws IOException {
        // set time
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();

        // preparing
        initWriter();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        batch = schema.createRowBatch();

        // generate values
        int counter = 0, line_num = lineNumber;

//        if(nullRate == (float) 0.5){
//            line_num = 2;
//        }else if(nullRate == (float) 0.67){
//            line_num = 3;
//        }else if(nullRate == (float) 0.75){
//            line_num = 4;
//        }else if(nullRate == (float) 0.8){
//            line_num = 5;
//        }
//


        int time = 0;
        int row;
        while(counter < ptNum){
//            System.out.println(counter);
            int index = 0;
            for(int i = 0; i < line_num - 1; i++){
                row = batch.size++;

                for(int j = 1; j <= sensorNum; j++)
                    ((DoubleColumnVector) batch.cols[j]).vector[row] = Double.NaN;

                ((LongColumnVector) batch.cols[0]).vector[row] = ++time;
                realAllPnt++;
                ((DoubleColumnVector) batch.cols[i+1]).vector[row] = (float) dataGenerator.next();
                realAllPnt++;
                index++;
                checkAndFlush();
            }

            row = batch.size++;
            ((LongColumnVector) batch.cols[0]).vector[row] = ++time;
            realAllPnt++;
            for(int i = 0; i < index; i++) ((DoubleColumnVector) batch.cols[i + 1]).vector[row] = Double.NaN;
            for(int i = index; i < sensorNum; i++){
                ((DoubleColumnVector) batch.cols[i + 1]).vector[row] = (float) dataGenerator.next();
                realAllPnt++;
            }
            checkAndFlush();
            counter++;
        }


        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private void checkAndFlush() throws IOException {
        if(batch.size == batch.getMaxSize()){
            writer.addRowBatch(batch);
            batch.reset();
        }
    }


    public static void run(int lineNum) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;
        for (int i = 0; i < repetition; i++) {
            ORCGeneratorV2 orcGenerator = new ORCGeneratorV2();
            if (align)
                orcGenerator.gen(lineNum);
//            else
//                orcGenerator.genNonalign();
//            double avgSpd = (sensorNum * deviceNum * ptNum) / (orcGenerator.timeConsumption / 1000.0);
            double avgSpd = (realAllPnt) / (orcGenerator.timeConsumption / 1000.0);
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


    public static void exper(int lab, int x, int line_number) throws IOException {
//        nullRate = rate;
        String exInfo = "d" + deviceNum + "_s" + x + "_r" + ptNum +"_line" + line_number;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
        filePath = exInfo + ".orc";

        align = true;
//        deviceNum = 100;
        sensorNum = x * deviceNum; // it includes all the sensors in the system
        repetition = 1;
        keepFile = true;
        try {
            run(line_number);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }

    /**
     *
     * @param args, # lab, device #,  # sensor per device, line, ptNum
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
//        args = new String[]{"1", "1", "10", "5", "10"};
        int lab_in = Integer.parseInt(args[0]);
        int deviceNum_in = Integer.parseInt(args[1]);
        int sensorPerDevice_in = Integer.parseInt(args[2]);
        int lineNum = Integer.parseInt(args[3]);
        int ptNum_in = Integer.parseInt(args[4]);


        ptNum = ptNum_in;

        deviceNum = deviceNum_in;
        expReportFilePath = "orc_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(lab_in, sensorPerDevice_in, lineNum);

        reportWriter.close();

    }
}
