package expr.nodevice;

import org.apache.hadoop.fs.Path;
import org.apache.orc.*;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import static cons.Constants.*;

public class ORCQuerierV2 {

    public static String genSchema(int col){
        StringBuilder res  = new StringBuilder("struct<time:bigint");
        for(int i = 0; i < col; i++) res.append(",s"+i+":float") ;
        res.append(">");
        return res.toString();
    }


    /**
     *
     * @param args filename, colNum, hasTimeFilter, maxTime, hasValFilter, maxVal
     *             parquet_lab1_x2_rate0.0.parquet 5 0.5 12
     * @throws IOException
     */
    public static void main(String args[]) throws IOException {
        args = new String[]{"d1_s10_r10_line1.orc", "5", "true", "10", "true", "10"};
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

        TypeDescription readSchema = TypeDescription.fromString(genSchema(selectNumber));

        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options()
                .schema(readSchema));

        long up = (long)(selectRate * ptNum);

        boolean isFinish = false;

        while (rowIterator.nextBatch(batch)) {
            for(int r=0; r < batch.size; ++r) {
                Object[] line = new Object[selectNumber+1];
                long t = ((LongColumnVector)batch.cols[0]).vector[r];
                if(t >= up){
                    isFinish = true;
                    break;
                }
                line[0] = t;
                for(int i = 1; i <= selectNumber; i++) {
                    line[i] = ((DoubleColumnVector)batch.cols[i]).vector[r];
                }
//                System.out.println(Arrays.toString(line));
            }
            if(isFinish) break;
        }
        rowIterator.close();


        long time = System.currentTimeMillis() - start;
        rptWrite.write("query " + fileString + "\n");
        rptWrite.write("# select col: " + selectNumber + "\t");
        rptWrite.write("# select rate: " + selectRate + "\t");
        rptWrite.write("# ptNum: " + ptNum + "\t");
        rptWrite.write("time: " + time + "ms\n\n");
        rptWrite.close();
    }
}
