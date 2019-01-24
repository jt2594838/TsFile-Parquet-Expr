package expr.generatefromcsv;

import com.csvreader.CsvReader;
import expr.MonitorThread;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.*;
import static cons.Constants.expReportFilePath;

public class ParquetGeneratorFromCSV {
    private ParquetWriter writer;
    private MessageType schema;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private static FileWriter reportWriter;

    static CsvReader reader;

    public ParquetGeneratorFromCSV(){}

    private void init() throws IOException {

        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        for (int j = 0; j < sensorNum; j++) {
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));
        }

        schema = builder.named(schemaName);

        GroupWriteSupport.setSchema(schema, configuration);
        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        groupWriteSupport.init(configuration);
        new File(filePath).delete();
        writer = new ParquetWriter(new Path(filePath), groupWriteSupport, CompressionCodecName.SNAPPY,
                ParquetWriter.DEFAULT_BLOCK_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE, ParquetWriter.DEFAULT_PAGE_SIZE,
                usingEncoing, true, ParquetProperties.WriterVersion.PARQUET_2_0);
    }

    public void gen() throws IOException {
        monitorThread = new MonitorThread();
        monitorThread.start();
        long startTime = System.currentTimeMillis();
        init();
        SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);


        while(reader.readRecord()){
            String[] line = reader.getValues();
            Group group = simpleGroupFactory.newGroup();
            group.add("time", Long.parseLong(line[0]));
            realAllPnt++;
            for(int i = 1; i <line.length; i++){
                if(line[i] == "") continue;
                group.add(SENSOR_PREFIX + (i-1), Float.parseFloat(line[i]));
                realAllPnt++;
            }
            writer.write(group);
        }

        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run(String csvPath) throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        reader = new CsvReader(csvPath);
        realAllPnt = 0;
        for (int i = 0; i < repetition; i ++) {
            ParquetGeneratorFromCSV parquetGeneratorFromCSV = new ParquetGeneratorFromCSV();
            if (align)
                parquetGeneratorFromCSV.gen();

            double avgSpd = (realAllPnt) / (parquetGeneratorFromCSV.timeConsumption / 1000.0);
            double memUsage = parquetGeneratorFromCSV.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
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
        filePath = exInfo + ".parquet";
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

    public static void main(String[] args) throws IOException {
        String csv_path = args[0];
        expReportFilePath = "pq_rpt";
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
