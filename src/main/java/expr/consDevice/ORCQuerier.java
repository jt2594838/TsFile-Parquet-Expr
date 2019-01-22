package expr.consDevice;

import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.io.IOException;


import static cons.Constants.*;

public class ORCQuerier {
    private long timeConsumption;


    public void query() throws IOException {
        Reader reader = OrcFile.createReader(new Path("my-file.orc"),
                OrcFile.readerOptions(configuration));
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        int cnt = 0;
        long timeThreshold = (long) (ptNum * selectRate);

        if(useFilter){
            while (rows.nextBatch(batch)) {
                for(int r=0; r < batch.size; ++r) {
                    //    ... process row r from batch
                    if(useFilter){
//                        long time =
                    }

                }
            }

        }else{


        }



        rows.close();

    }


    private static void run() throws IOException{
        long totConsumption = 0;
        for(int i = 0; i < repetition; i++){
            ORCQuerier q = new ORCQuerier();
            q.query();
            totConsumption += q.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totConsumption / repetition));
    }



    public static void main(String args[]){
        filePath = "exper2.orc";
        useFilter = false;
        ptNum = 10000;
        selectNum = 5;
        selectRate = 0.1;
        repetition = 1;
        try {
            run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
