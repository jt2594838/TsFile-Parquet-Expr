package trash;


//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.orc.CompressionKind;
//import org.apache.orc.OrcFile;
//import org.apache.orc.TypeDescription;
//import org.apache.orc.Writer;
//import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
//import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

public class Test {

    public static void main(String[] args) throws Exception {
        for(int j = 0; j < 10; j++)
            for(int i = 0; i < 10000; i++)
                if(Math.random() >= 1)
                    System.out.println("---");


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