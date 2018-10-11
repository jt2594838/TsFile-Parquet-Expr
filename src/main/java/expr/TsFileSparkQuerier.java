package expr;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static cons.Constants.*;
import static cons.Constants.selectNum;


public class TsFileSparkQuerier {
    private long timeConsumption;

    public void query() {
        SparkSession spark = SparkSession.builder().appName(SESSION_NAME).master("local").getOrCreate();
        Dataset<Row> parquetDataset = null;
        String viewName = "TsFile";
        spark.sql("create temporary view " + viewName + " using cn.edu.tsinghua.tsfile options(path = \"" + filePath + "\")");

        String sparkSqlFilterTemplate = "SELECT %s FROM %s WHERE " + viewName +".delta_object=\"d0\" and %s";
        String sparkSqlTemplate = "SELECT %s FROM %s WHERE " + viewName + ".delta_object=\"d0\"";
        StringBuilder columns = new StringBuilder("");
        for (int i = 0; i < selectNum; i++) {
            columns.append(viewName).append(SEPARATOR).append(SENSOR_PREFIX).append(i);
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
            sparkSql = String.format(sparkSqlFilterTemplate, columns.toString(), viewName, filter);
        } else {
            sparkSql = String.format(sparkSqlTemplate, columns.toString(), viewName);
        }


        long startTime = System.currentTimeMillis();
        Dataset<Row> dataset = spark.sql(sparkSql);
        System.out.println(dataset.count());
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            TsFileSparkQuerier test = new TsFileSparkQuerier();
            test.query();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }

    public static void main(String[] args) {
        filePath = "expr2.ts";
        useFilter = true;
        selectNum = 5;
        run();
    }
}