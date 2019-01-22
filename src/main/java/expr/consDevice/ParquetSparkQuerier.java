package expr.consDevice;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cons.Constants.*;

public class ParquetSparkQuerier {

    private long timeConsumption;

    public void query() {
        SparkSession spark = SparkSession.builder().appName(SESSION_NAME).master("local").getOrCreate();
        Dataset<Row> parquetDataset = spark.read().parquet(filePath);
        parquetDataset.createOrReplaceTempView("parquetFile");

        String sparkSqlFilterTemplate = "SELECT %s FROM parquetFile WHERE %s";
        String sparkSqlTemplate = "SELECT %s FROM parquetFile";
        StringBuilder columns = new StringBuilder("");
        for (int i = 0; i < selectNum; i++) {
            columns.append(DEVICE_PREFIX).append(i).append(SEPARATOR).append(SENSOR_PREFIX).append(i);
            if (i != selectNum - 1) {
                columns.append(",");
            }
        }
        String sparkSql;
        if (useFilter) {
            String filter="";
            if (align)
                filter = String.format("time < %d",(long) (ptNum * selectRate));
            else
                filter = String.format("time < %d", (long) ((ptNum * selectRate + 1) * sensorNum));
            sparkSql = String.format(sparkSqlFilterTemplate, columns.toString(), filter);
        } else {
            sparkSql = String.format(sparkSqlTemplate, columns.toString());
        }


        long startTime = System.nanoTime();
        Dataset<Row> dataset = spark.sql(sparkSql);
        timeConsumption = System.nanoTime() - startTime;
    }

    private static void run() {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            ParquetSparkQuerier test = new ParquetSparkQuerier();
            test.query();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dns", totContumption / repetition));
    }

    public static void main(String[] args) {
        filePath = "exper2sp_cd.parquet";
        useFilter = false;
        selectNum = 1;
        run();
    }
}
