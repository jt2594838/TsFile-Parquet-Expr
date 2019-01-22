package expr.consDevice;


import datagen.DataGenerator;
import datagen.GeneratorFactory;
import expr.MonitorThread;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.File;
import java.io.IOException;

import static cons.Constants.*;


/**
 * In this experiment, we consider the conception of a device.
 * The table is designed as (time, d, s1, s2, ..., sn)
 */
public class ORCGenerator {

    private MonitorThread monitorThread;
    private long timeConsumption;
    private DataGenerator dataGenerator;
    private Writer writer;
    private TypeDescription schema;

    public ORCGenerator() {

    }

    /**
     * To generate a string, which describe the structure of the table (schema).
     * For example, "struct<time:bigint,s1:float,s2:float>"
     *
     * @return
     */
    private static String genStringSchema() {
        String s = "struct<time:bigint," + DEVICE_PREFIX + ":int";
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
    private String[] splitStrSchema(String s) {
        int t = -1;
        for (int i = s.length() - 1; i >= 0; i--) {
            if (s.toCharArray()[i] == '>') {
                t = i;
                break;
            }
        }
        String ss = s.substring(s.indexOf("<") + 1, t);
        String[] a = ss.split(",");
        String[] ans = new String[a.length];
        for (int i = 0; i < ans.length; i++) {
            ans[i] = a[i].split(":")[1];
        }
        return ans;
    }

    /**
     * Initialize writer object
     *
     * @throws IOException
     */
    private void initWriter() throws IOException {
        schema = TypeDescription.fromString(genStringSchema());
        new File(filePath).delete();
        writer = OrcFile.createWriter(new Path(filePath), OrcFile.writerOptions(configuration).setSchema(schema));
    }

    /**
     * Generate data and write data into database
     *
     * @throws IOException
     */
    private void gen() throws IOException {
        // set time
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();

        // preparing
        initWriter();
        dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        VectorizedRowBatch batch = schema.createRowBatch();
        String[] a = splitStrSchema(genStringSchema());
        ColumnVector[] vcs = new ColumnVector[a.length];
        for (int i = 0; i < vcs.length; i++)
            vcs[i] = batch.cols[i];

        // generate values
        // TODO type change, big data test
        float[] sensors;
        long counter = 0;
        for (int k = 0; k < ptNum; k++) {
            for (int i = 0; i < deviceNum; i++) {
                // generate value
                LongColumnVector time = (LongColumnVector) vcs[0];   // get reference of column TABLE
                LongColumnVector device = (LongColumnVector) vcs[1]; // get reference of column DEVICE
                time.vector[(int) counter] = k + 1;  // assign data to column TABLE
                device.vector[(int) counter] = i;   // assign data to column DEVICE
                for (int j = 0; j < sensorNum; j++) { //  a loop to assign data to all sensor columns
                    DoubleColumnVector v = (DoubleColumnVector) vcs[j + 2];
                    v.vector[(int) counter] = (float) dataGenerator.next();
                }
                //update counter
                counter = batch.size++;
                // If the batch is full, write it out and start over.
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

    // TODO
    private void genNonalign() throws IOException {

    }

    private static void run() throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        for (int i = 0; i < repetition; i++) {
            ORCGenerator orcGenerator = new ORCGenerator();
            if (align)
                orcGenerator.gen();
            else
                orcGenerator.genNonalign();
            double avgSpd = (sensorNum * deviceNum * ptNum) / (orcGenerator.timeConsumption / 1000.0);
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
    }


    public static void main(String args[]) {
        filePath = "exper_cd.orc";
        align = true;
        deviceNum = 100;
        sensorNum = 100;
        repetition = 1;
        keepFile = true;
        try {
            run();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
