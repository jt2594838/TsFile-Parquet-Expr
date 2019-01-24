package expr.generatefromcsv;

import com.csvreader.CsvReader;
import datagen.GeneratorFactory;
import expr.MonitorThread;
import expr.nodevice.ORCGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static cons.Constants.*;
import static cons.Constants.expReportFilePath;
import static cons.Constants.ptNum;

public class OrcGeneratorFromCSV {
    private MonitorThread monitorThread;
    private long timeConsumption;
    private Writer writer;
    private TypeDescription schema;

    private static FileWriter reportWriter;

    static CsvReader reader;

    public OrcGeneratorFromCSV() {
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
    private void gen() throws IOException {
        // set time
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();

        // preparing
        initWriter();
        VectorizedRowBatch batch = schema.createRowBatch();


        int row = 0;
        while(reader.readRecord()){
            String[] line = reader.getValues();
            row = batch.size++;
            ((LongColumnVector) batch.cols[0]).vector[row] = Long.parseLong(line[0]);
            realAllPnt++;
            for(int i = 1; i <= line.length - 1; i++){
                if(line[i] == ""){
                    continue;
                }
                else{
                    ((DoubleColumnVector) batch.cols[i]).vector[row] = Float.parseFloat(line[i]);
                    realAllPnt++;
                }
            }
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
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


    public static void run(String csvPath) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;
        reader = new CsvReader(csvPath);

        for (int i = 0; i < repetition; i++) {
            OrcGeneratorFromCSV orcGeneratorFromCSV = new OrcGeneratorFromCSV();
            if (align)
                orcGeneratorFromCSV.gen();
            double avgSpd = (realAllPnt) / (orcGeneratorFromCSV.timeConsumption / 1000.0);
            double memUsage = orcGeneratorFromCSV.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
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
        System.out.println(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));

        reportWriter.write(String.format("FileName: %s; DataType: %s; Encoding: %s\n", filePath, typeName, usingEncoing));
        reportWriter.write(String.format("Total Avg speed : %fpt/s; Total max memory usage: %fMB; File size: %fMB\n",
                totAvgSpd / repetition, totMemUsage / repetition, totFileSize / repetition));
        reportWriter.write("\n");
    }


    public static void exper(String csvPath) throws IOException {
        System.out.println(csvPath);
        String exInfo = csvPath.split("[.]")[0] + csvPath.split("[.]")[1];
        reportWriter.write(exInfo + ":\n");
        filePath = exInfo + ".orc";
        align = true;
        repetition = 1;
        keepFile = true;
        try {
            run(csvPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }

    /**
     *
     * @param args, csv path
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        String csv_path = args[0];
        System.out.println(csv_path);
        expReportFilePath = "orc_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);

        CsvReader preReader = new CsvReader(csv_path);
        preReader.readRecord();
        sensorNum = preReader.getValues().length - 1;


        exper(csv_path);
        reportWriter.close();
    }
}
