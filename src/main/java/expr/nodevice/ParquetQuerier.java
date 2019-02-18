package expr.nodevice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.IOException;

import static cons.Constants.*;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetQuerier {

    private static long timeConsumption;

    public static void query() throws IOException {

        long startTime = System.currentTimeMillis();

        ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, new Path(filePath), NO_FILTER);
        MessageType schema = metadata.getFileMetaData().getSchema();
        ParquetFileReader fileReader = new ParquetFileReader(configuration, new Path(filePath), metadata);


        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        for (int j = 0; j < selectNum; j++)
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));

        MessageType querySchema = builder.named(schemaName);


        PageReadStore pages = null;
        int cnt = 0;

        Operators.LongColumn time = longColumn("time");
        FilterPredicate filter = lt(time, (long)(ptNum * selectRate));



        while ((pages = fileReader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(querySchema, schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(querySchema), FilterCompat.get(filter));

//            recordReader

            for(int i = 0; i < pages.getRowCount(); i++) {
                System.out.println(pages.getRowCount());
                Group group = recordReader.read();
                System.out.println(group.toString());
//
//                if (useFilter) {
//                    long time = group.getLong("time", 0);
//                    if(time > timeThreshold)
//                        continue;
//                }
//                String device = group.getString("device", 0);
//                if (device.equals(DEVICE_PREFIX + 0)) {
//                    cnt++;
//                    // System.out.println(group);
//                }
            }
        }
        timeConsumption = System.currentTimeMillis() - startTime;
    }


    /*input series per device*/
    // arquet_lab1_x2_rate0.0.parquet 0.5 5
    public static void main(String args[]) throws IOException {
        filePath = args[0];
        ptNum = 12;
        selectRate = Float.parseFloat(args[1]);
        useFilter = true;
        deviceNum = 100;
        sensorNum = Integer.parseInt(args[0].split("_")[2].split("x")[1]);
        sensorNum = deviceNum * sensorNum;
        selectNum = Integer.parseInt(args[2]);
        query();

    }


}
