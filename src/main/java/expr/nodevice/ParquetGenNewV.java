package expr.nodevice;

import datagen.DataGenerator;
import datagen.GeneratorFactory;
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

public class ParquetGenNewV {
    private ParquetWriter writer;
    private MessageType schema;
    private DataGenerator dataGenerator;
    private MonitorThread monitorThread;
    private long timeConsumption;
    private static FileWriter reportWriter;

    public ParquetGenNewV(){}

    private void init() throws IOException {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        for (int j = 0; j < sensorNum; j++)
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));

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
            int index = 0;
            for(int i = 0; i < line_num - 1; i++){
                Group group = simpleGroupFactory.newGroup();
                group.add("time", ++time);
                realAllPnt++;
                group.add(SENSOR_PREFIX + index, (float) dataGenerator.next());
                realAllPnt++;
                index++;
                writer.write(group);
            }

            Group group = simpleGroupFactory.newGroup();

            realAllPnt++;
            group.add("time", ++time);
            for(int i = index; i < sensorNum; i++){
                group.add(SENSOR_PREFIX + i, (float) dataGenerator.next());
                realAllPnt++;
            }
            writer.write(group);
            counter++;
        }

        writer.close();
        monitorThread.interrupt();
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException {
        double totAvgSpd = 0.0, totMemUsage = 0.0, totFileSize = 0.0;
        realAllPnt = 0;

        ParquetGenNewV p = new ParquetGenNewV();
        if (align)
            p.gen();

        double avgSpd = (realAllPnt) / (p.timeConsumption / 1000.0);
        double memUsage = p.monitorThread.getMaxMemUsage() / (1024.0 * 1024.0);
        totAvgSpd += avgSpd;
        totMemUsage += memUsage;
        System.out.println(String.format("ParquetFile generation completed. avg speed : %fpt/s, max memory usage: %fMB",
                avgSpd, memUsage));
        File file = new File(filePath);
        totFileSize += file.length() / (1024.0 * 1024.0);
        if (!keepFile) {
            file.delete();
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

    public static void exper(int lab, int seriesPerDevice, float rate) throws IOException {
        nullRate = rate;
//        String exInfo = "parquet_lab" + lab + "_x" + seriesPerDevice + "_rate" + rate;
        String exInfo = "d" + deviceNum + "_s" + seriesPerDevice + "_r" + ptNum + "_rate" + rate;
        reportWriter.write(exInfo + ":\n");
        System.out.println(exInfo + "begins........");
        filePath = exInfo + ".parquet";
        align = true;
//        deviceNum = 100; // TODO
        sensorNum = seriesPerDevice * deviceNum; // it includes all the sensors in the system
        repetition = 1;
        keepFile = true;
        try {
            run();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(exInfo + "finishes.");
        System.out.println();

    }


    /**
     *
     * @param args, exp #, device #,sen1sorPerDevice, nullrate, rows;
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
//        args = args = new String[]{"1", "100", "0", "1000"};
        int lab_in = Integer.parseInt(args[0]),
                device_in = Integer.parseInt(args[1]),
                seriesPerDevice_in = Integer.parseInt(args[2]) ,
                ptNum_in = Integer.parseInt(args[4]);
        float nullRate_in = Float.parseFloat(args[3]);
        ptNum = ptNum_in;

        deviceNum = device_in;
        expReportFilePath = "parquet_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);
        exper(lab_in, seriesPerDevice_in, nullRate_in);


        reportWriter.close();
    }

}
