package expr.nodevice;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.*;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ParquetQ_v1 {

    private static FileWriter rptWrite;

    public static void run() throws IOException {

        long start = System.currentTimeMillis();

        ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, new Path(filePath), NO_FILTER);
        MessageType schema = metadata.getFileMetaData().getSchema();

        ParquetInputFormat.setFilterPredicate(configuration, lt(longColumn("time"), (long)(ptNum*selectRate)));
        FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);

        ParquetFileReader fileReader = new ParquetFileReader(configuration, new Path(filePath), metadata);

        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        for(int i = 0; i < selectNum; i++)
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "s" + i));
        MessageType querySchema = builder.named(schemaName);
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(querySchema, schema);
        RecordReader<Group> recordReader;

        PageReadStore pages;
        while((pages = fileReader.readNextRowGroup())!= null){
            recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(querySchema), filter);
            for(int i = 0; i < pages.getRowCount(); i++){
                Group line = recordReader.read();
                if(line == null) break;
            }
        }

        long time = System.currentTimeMillis() - start;
        rptWrite.write("query " + filePath + "\n");
        rptWrite.write("# select col: " + selectNum + "\t");
        rptWrite.write("# select rate: " + selectRate + "\t");
        rptWrite.write("# ptNum: " + ptNum + "\t");
        rptWrite.write("time: " + time + "ms\n\n");

    }

    /**
     *
     * @param args, filename, colNum, seleRate, ptNum
     *              parquet_lab1_x2_rate0.0.parquet 5 0.5 12
     */
    public static void main(String args[]) throws IOException {
        filePath = args[0];
        selectNum = Integer.parseInt(args[1]);
        selectRate = Float.parseFloat(args[2]);
        ptNum = Long.parseLong(args[3]);

        File rpt = new File("parque_rpt");
        if(!rpt.exists()) rpt.createNewFile();
        rptWrite = new FileWriter(rpt, true);

        run();
        rptWrite.close();
    }
}
