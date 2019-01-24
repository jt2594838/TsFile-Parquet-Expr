package trash;



import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;
import hadoop.HDFSInputStream;
////import org.apache.hadoop.fs.Path;
//import org.apache.orc.OrcFile;
//import org.apache.orc.Reader;
//import org.apache.orc.RecordReader;
//import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
//import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
//import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
//import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
//import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

import java.util.ArrayList;
import java.util.List;

import static cons.Constants.*;

public class Test {

    public static void main(String[] args) throws Exception {
//        Reader reader =  OrcFile.createReader(new Path("data.orc"),
//                OrcFile.readerOptions(configuration));
//        RecordReader rows = reader.rows();
//        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
//        while (rows.nextBatch(batch)) {
//            for(int r=0; r < batch.size; ++r) {
//                System.out.println(((LongColumnVector)batch.cols[0]).vector[r] + " ");
//                for(int i = 1; i < batch.cols.length; i++)
//                    System.out.print(((DoubleColumnVector)batch.cols[i]).vector[r] + " ");
//                System.out.println();
//            }
//        }
//        rows.close();


        ITsRandomAccessFileReader reader = new HDFSInputStream("p10_d5_s2_r0.ts");
        QueryExecutorRouter router = new QueryExecutorRouter(new MetadataQuerierByFileImpl(reader),
                new SeriesChunkLoaderImpl(reader));
        List<Path> selectPaths = new ArrayList<>();
        for(int i = 0; i < 5; i++){
            for(int j = 0; j < 2; j++){
                selectPaths.add(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + j));
            }
        }
        QueryExpression expression = QueryExpression.create();
        expression.setSelectSeries(selectPaths);
        QueryDataSet dataSet = router.execute(expression);
        while(dataSet.hasNext()){
            System.out.println(dataSet.next().toString());
        }


    }

}





//        TypeDescription schema = TypeDescription.createStruct()
//                .addField("field1", TypeDescription.createString())
//                .addField("field2", TypeDescription.createString())
//                .addField("field3", TypeDescription.createString());
//
//        String lxw_orc1_file = "/tmp/lxw_orc1_file.orc";
//        Configuration conf = new Configuration();
//        FileSystem.getLocal(conf);
//        Writer writer = OrcFile.createWriter(new Path(lxw_orc1_file),
//                OrcFile.writerOptions(conf)
//                        .setSchema(schema)
//                        .stripeSize(67108864)
//                        .bufferSize(131072)
//                        .blockSize(134217728)
//                        .compress(CompressionKind.ZLIB)
//                        .version(OrcFile.Version.V_0_12));
//        //要写入的内容
//        String[] contents = new String[]{"1,a,aa","2,b,bb","3,c,cc","4,d,dd"};
//        VectorizedRowBatch batch = schema.createRowBatch();
//        for(String content : contents) {
//            int rowCount = batch.size++;
//            String[] logs = content.split(",", -1);
//            for(int i=0; i<logs.length; i++) {
//                ((BytesColumnVector) batch.cols[i]).setVal(rowCount, logs[i].getBytes());
//                //batch full
//                if (batch.size == batch.getMaxSize()) {
//                    writer.addRowBatch(batch);
//                    batch.reset();
//                }
//            }
//        }
//        writer.addRowBatch(batch);
//        writer.close();