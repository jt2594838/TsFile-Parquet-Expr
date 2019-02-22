package trash;



//import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
//import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
//import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;
//import hadoop.HDFSInputStream;
//import java.util.ArrayList;
//import java.util.List;
import static cons.Constants.*;
//import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
//import static org.apache.parquet.filter2.predicate.FilterApi.lt;
//import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
//
//import org.apache.parquet.column.ColumnDescriptor;
//import org.apache.parquet.column.page.PageReadStore;
//import org.apache.parquet.example.data.Group;
//import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
//import org.apache.parquet.filter.ColumnPredicates;
//import org.apache.parquet.filter.ColumnRecordFilter;
//import org.apache.parquet.filter.PagedRecordFilter;
//import org.apache.parquet.filter.UnboundRecordFilter;
//import org.apache.parquet.filter2.compat.FilterCompat;
//import org.apache.parquet.filter2.predicate.FilterPredicate;
//import org.apache.parquet.filter2.predicate.Operators;
//import org.apache.parquet.format.converter.ParquetMetadataConverter;
//import org.apache.parquet.hadoop.ParquetFileReader;
//import org.apache.parquet.hadoop.ParquetInputFormat;
//import org.apache.parquet.hadoop.ParquetReader;
//import org.apache.parquet.hadoop.api.ReadSupport;
//import org.apache.parquet.hadoop.example.GroupReadSupport;
//import org.apache.hadoop.fs.Path;
//import org.apache.parquet.hadoop.example.GroupWriteSupport;
//import org.apache.parquet.hadoop.metadata.ParquetMetadata;
//import org.apache.parquet.io.ColumnIOFactory;
//import org.apache.parquet.io.MessageColumnIO;
//import org.apache.parquet.io.RecordReader;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.Type;
//import org.apache.parquet.schema.Types;

//import java.awt.*;
//import java.util.HashMap;
//import java.util.Map;

import ch.qos.logback.core.subst.Node;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.storage.ql.io.sarg.PredicateLeaf;
import org.apache.orc.storage.ql.io.sarg.SearchArgument;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.storage.ql.io.sarg.SearchArgumentImpl;

import javax.swing.text.html.Option;
import java.sql.Timestamp;
import java.util.List;


public class Test {

//    public static PredicateLeaf createPredicateLeaf(PredicateLeaf.Operator operator,
//                                                    PredicateLeaf.Type type,
//                                                    String columnName,
//                                                    Object literal,
//                                                    List<Object> literalList) {
//        return new SearchArgumentImpl.PredicateLeafImpl(operator, type, columnName,
//                literal, literalList);
//    }

    public static void main(String[] args) throws Exception {
        Reader reader =  OrcFile.createReader(new Path("d1_s10_r100000_rate0.0.orc"),
                OrcFile.readerOptions(configuration));

        TypeDescription readSchema = TypeDescription.fromString("struct<time:bigint,s0:float,s1:float>");


        SearchArgument sarg = SearchArgumentFactory.newBuilder().equals("time", PredicateLeaf.Type.LONG, (long)3).build();
        Reader.Options readerOptions = new Reader.Options(configuration)
                .searchArgument(sarg, new String[]{"time"})
                .schema(readSchema);

        RecordReaderImpl rows = (RecordReaderImpl) reader.rows(readerOptions);

        VectorizedRowBatch batch = readSchema.createRowBatch();
        RecordReader rowIterator = reader.rows(reader.options()
                .schema(readSchema));


//        PredicateLeaf pred = PredicateLeaf.createPredicateLeaf(PredicateLeaf.Operator.NULL_SAFE_EQUALS, PredicateLeaf.Type.TIMESTAMP, "x",
//                Timestamp.valueOf("2007-08-01 02:00:00.0"), null);
//        Assert.assertEquals(SearchArgument.TruthValue.NO, RecordReaderImpl.evaluatePredicate(colStats[1], pred, bf));

        // size of batch is 1024
        while (rowIterator.nextBatch(batch)) {
//
            for(int r=0; r < batch.size; ++r) {
//                System.out.println(batch.size);
//                System.out.print(((LongColumnVector)batch.cols[0]).vector[r] + " ");
                for(int i = 1; i < batch.cols.length; i++);
//                    System.out.print(((DoubleColumnVector)batch.cols[i]).vector[r] + " ");
//                System.out.println();
            }
        }
        rows.close();
//
//
//        Reader reader =  OrcFile.createReader(new Path("d1_s10_r10_line5.orc"),
//                OrcFile.readerOptions(configuration));
//        RecordReader rows = reader.rows();
//        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
//        while (rows.nextBatch(batch)) {
//            for(int r=0; r < batch.size; ++r) {
//                System.out.print(((LongColumnVector)batch.cols[0]).vector[r] + " ");
//                for(int i = 1; i < batch.cols.length; i++)
//                    System.out.print(((DoubleColumnVector)batch.cols[i]).vector[r] + " ");
//                System.out.println();
//            }
//        }
//        rows.close();


//
//        ITsRandomAccessFileReader reader = new HDFSInputStream("ts_lab2_x2_rate0.67.ts");
////        ITsRandomAccessFileReader reader = new HDFSInputStream("F:\\idea_workspace\\WriteAndRead\\file.ts");
//        QueryExecutorRouter router = new QueryExecutorRouter(new MetadataQuerierByFileImpl(reader),
//                new SeriesChunkLoaderImpl(reader));
//        List<Path> selectPaths = new ArrayList<>();
//        for(int i = 0; i < 5; i++){
//            for(int j = 0; j < 2; j++){
//                selectPaths.add(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + j));
//            }
//        }
//        QueryExpression expression = QueryExpression.create();
//        expression.setSelectSeries(selectPaths);
//        QueryDataSet dataSet = router.execute(expression);
//        while(dataSet.hasNext()){
//            System.out.println(dataSet.next().toString());
//        }



//
//        // set filter
//        ParquetInputFormat.setFilterPredicate(configuration, lt(longColumn("time"), (long)(5)));
//        FilterCompat.Filter filter = ParquetInputFormat.getFilter(configuration);
//
//        // set schema
//        Types.MessageTypeBuilder builder = Types.buildMessage();
//        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
//        for(int i = 0; i < 9; i++)
//            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "s" + i));
//        MessageType querySchema = builder.named(schemaName);
//        configuration.set(ReadSupport.PARQUET_READ_SCHEMA, querySchema.toString());
//
//        // set reader
//        ParquetReader.Builder<Group> reader= ParquetReader
//                .builder(new GroupReadSupport(), new Path("parquet_lab1_x2_rate0.0.parquet"))
//                .withConf(configuration)
//                .withFilter(filter);
//
//        // read
//        ParquetReader<Group> build=reader.build();
//        Group line;
//        while((line=build.read())!=null){
//            System.out.println(line.toString());
//        }



//        ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, new Path("parquet_lab1_x2_rate0.0.parquet"), NO_FILTER);
//        MessageType schema = metadata.getFileMetaData().getSchema();
//
//        Operators.LongColumn time = longColumn("time");
//        FilterPredicate filter = lt(time, (long)5);
//        ParquetInputFormat.setFilterPredicate(configuration, filter);
//        FilterCompat.Filter f = ParquetInputFormat.getFilter(configuration);
//
//        ParquetFileReader fileReader = new ParquetFileReader(configuration, new Path("parquet_lab1_x2_rate0.0.parquet"), metadata);
//
//        Types.MessageTypeBuilder builder = Types.buildMessage();
//        builder.addField(new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "time"));
//        for(int i = 0; i < 3; i++)
//            builder.addField(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveType.PrimitiveTypeName.FLOAT, "s" + i));
//        MessageType quertySchema = builder.named(schemaName);
//        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(quertySchema, schema);
//        RecordReader<Group> recordReader;
//
//        PageReadStore pages;
//        while((pages = fileReader.readNextRowGroup())!= null){
//            recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(quertySchema), f);
//            for(int i = 0; i < pages.getRowCount(); i++){
//                Group line = recordReader.read();
//                if(line == null) break;
//                System.out.println(line.toString());
//            }
//        }


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