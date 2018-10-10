package expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cons.Constants.*;

public class ParquetQuerier {

    private long timeConsumption;

    public void query() {
        SparkSession spark = SparkSession.builder().appName(SESSION_NAME).getOrCreate();
        Dataset<Row> parquetDataset = spark.read().parquet(filePath);
        parquetDataset.createOrReplaceTempView("parquetFile");

        String sparkSqlTemplate = "SELECT %s FROM parquetFile";
        StringBuilder columns = new StringBuilder();
        for (int i = 0; i < selectNum; i++) {
            columns.append(DEVICE_PREFIX).append(i).append(SEPARATOR).append(SENSOR_PREFIX).append(i);
            if (i != selectNum - 1) {
                columns.append(",");
            }
        }
        String sparkSql = String.format(sparkSqlTemplate, columns.toString());

        long startTime = System.currentTimeMillis();
        Dataset<Row> dataset = spark.sql(sparkSql);
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            ParquetQuerier test = new ParquetQuerier();
            test.query();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }
}
