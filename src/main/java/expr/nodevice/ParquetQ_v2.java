package expr.nodevice;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
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

public class ParquetQ_v2 {

    private static FileWriter rptWrite;

    private static void run() throws IOException {
        long start = System.currentTimeMillis();

        // set filter
        ParquetInputFormat.setFilterPredicate(configuration, lt(longColumn("time"), (long)(ptNum*selectRate)));
        FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);

        // set schema
        Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
        for(int i = 0; i < selectNum; i++)
            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "s" + i));
        MessageType querySchema = builder.named(schemaName);
        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());

        // set reader
        ParquetReader.Builder<Group> reader= ParquetReader
                .builder(new GroupReadSupport(), new Path(filePath))
                .withConf(configuration)
                .withFilter(filter);

        // read
        ParquetReader<Group> build=reader.build();
        Group line;
        while((line=build.read())!=null){
//            System.out.println(line.toString());
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
