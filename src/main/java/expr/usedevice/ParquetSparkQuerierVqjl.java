package expr.usedevice;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cons.Constants.*;

public class ParquetSparkQuerierVqjl {

    private long timeConsumption;
    private static String sql = "select * from parquetFile where id < 4";

    public void query() {
        SparkSession spark = SparkSession.builder().appName(SESSION_NAME).master("local").getOrCreate();
        Dataset<Row> parquetDataset = spark.read().parquet(filePath);
        parquetDataset.createOrReplaceTempView("parquetFile");

        spark.conf().set("spark.sql.parquet.enableVectorizedReader", "false");

        long startTime = System.currentTimeMillis();

        Dataset<Row> dataset = spark.sql(sql);

        dataset.show(10000);

//        dataset.cache();

//        dataset.collect();

        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            ParquetSparkQuerierVqjl test = new ParquetSparkQuerierVqjl();
            test.query();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }

    public static void main(String[] args) {
        filePath = "file_have_time.parquet";

        if (args.length == 2) {
            filePath = args[0];

            sql = args[1];
        }
        useFilter = false;
        selectNum = 1;
        run();
    }
}