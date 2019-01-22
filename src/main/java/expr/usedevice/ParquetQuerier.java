package expr.usedevice;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
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
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetQuerier {

    private long timeConsumption;

    public void query() throws IOException {

        long startTime = System.currentTimeMillis();

        ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, new Path(filePath), NO_FILTER);
        MessageType schema = metadata.getFileMetaData().getSchema();
        ParquetFileReader fileReader = new ParquetFileReader(configuration, new Path(filePath), metadata);

        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "device"));
        for (int j = 0; j < selectNum; j++) {
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, typeName,SENSOR_PREFIX + j));
        }
        MessageType querySchema = builder.named(schemaName);

        PageReadStore pages = null;
        int cnt = 0;
        long timeThreshold = (long) (ptNum * selectRate);
        while ((pages = fileReader.readNextRowGroup()) != null) {
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(querySchema, schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(querySchema));

            for(int i = 0; i < pages.getRowCount(); i++) {
                Group group = recordReader.read();
                if (useFilter) {
                    long time = group.getLong("time", 0);
                    if(time > timeThreshold)
                        continue;
                }
                String device = group.getString("device", 0);
                if (device.equals(DEVICE_PREFIX + 0)) {
                    cnt++;
                    // System.out.println(group);
                }
            }
        }
        System.out.println(cnt);
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            ParquetQuerier test = new ParquetQuerier();
            test.query();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }

    public static void main(String[] args) throws IOException {
        filePath = "expr2.parquet";
        useFilter = false;
        ptNum = 10000;
        selectNum = 5;
        selectRate = 0.1;
        repetition = 1;
        run();
    }
}