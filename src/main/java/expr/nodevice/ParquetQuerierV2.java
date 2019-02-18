package expr.nodevice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;

import static cons.Constants.SENSOR_PREFIX;
import static cons.Constants.sensorNum;
import static cons.Constants.typeName;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;

public class ParquetQuerierV2 {


    // parquet_lab1_x2_rate0.0.parquet 0.5 5
    public static void main(String args[]) throws IOException {
        String filePath = args[0];
        float seleRate = Float.parseFloat(args[1]);
        int colNum = Integer.parseInt(args[2]);

        System.out.println("start reading " + filePath + "...");

        File f = new File(filePath);
        if(!f.exists()) throw new FileNotFoundException();

        long startTime = System.currentTimeMillis();


        Configuration conf = new Configuration();
        Operators.LongColumn time = longColumn("time");
        FilterPredicate filter = lt(time, (long)(1000000 * seleRate));



        ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(),
                new Path(f.getAbsolutePath())).withConf(conf).withFilter(FilterCompat.get(filter)).build();

        Group line = null;

        int counter = 0;
        while((line = reader.read())!=null){
            for(int i = 0; i < colNum; i++)
                line.getFloat(SENSOR_PREFIX + i, 0);
            counter++;
        }

        System.out.println(counter);

        long duration = System.currentTimeMillis() - startTime;

        File rpt = new File("parque_rpt");
        if(!rpt.exists()) rpt.createNewFile();
        FileWriter rptWriter = new FileWriter(rpt, true);
        rptWriter.write("quering " + filePath + "\n");
        rptWriter.write("rate: " + seleRate + "     select rows: " + counter);
        rptWriter.write("Time Consumption: " + duration + "\n");
        rptWriter.close();
        System.out.println("reading " + filePath + " finished.");
    }
}
