package expr.nodevice;

import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.spark.sql.execution.datasources.orc.OrcOptions;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import static cons.Constants.*;
import static cons.Constants.ptNum;

public class ORCQuerier {




    /**
     *
     * @param args filename, colNum, seleRate, ptNum
     *             parquet_lab1_x2_rate0.0.parquet 5 0.5 12
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
//        args = new String[]{"d1_s10_r12_rate0.0.orc", "5", "0.5", "12"};
        String fileString = args[0];
        int selectNumber = Integer.parseInt(args[1]);
        float selectRate = Float.parseFloat(args[2]);
        int ptNum = Integer.parseInt(args[3]);
        FileWriter rptWrite;
        File rpt = new File("orc_rpt");
        if(!rpt.exists()) rpt.createNewFile();
        rptWrite = new FileWriter(rpt, true);



        long start = System.currentTimeMillis();

        Reader reader =  OrcFile.createReader(new Path(fileString),
                OrcFile.readerOptions(configuration));

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        Object[] line = new Object[selectNumber+1];

        long up = (long)(selectRate * ptNum);

        while (rows.nextBatch(batch)) {
            for(int r=0; r < batch.size; ++r) {
                long t = ((LongColumnVector)batch.cols[0]).vector[r];
                if(t >= up) continue;
                line[0] = t;
                for(int i = 1; i <= selectNumber; i++)
                    line[i] = ((DoubleColumnVector)batch.cols[i]).vector[r];
//                System.out.println(Arrays.toString(line));
            }
        }
        rows.close();


        long time = System.currentTimeMillis() - start;
        rptWrite.write("query " + fileString + "\n");
        rptWrite.write("# select col: " + selectNumber + "\t");
        rptWrite.write("# select rate: " + selectRate + "\t");
        rptWrite.write("# ptNum: " + ptNum + "\t");
        rptWrite.write("time: " + time + "ms\n\n");
        rptWrite.close();
    }
}
