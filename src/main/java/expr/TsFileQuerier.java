package expr;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.TimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;
import hadoop.HDFSInputStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cons.Constants.*;

public class TsFileQuerier {

    private QueryExecutorRouter router;
    private long timeConsumption;
    private static FileWriter reportWriter;

    private void initEngine() throws IOException {
        ITsRandomAccessFileReader reader = new HDFSInputStream(filePath);
        router = new QueryExecutorRouter(new MetadataQuerierByFileImpl(reader),
                                         new SeriesChunkLoaderImpl(reader));
    }

    private void testQuery() throws IOException {
        initEngine();

        List<Path> selectPaths = new ArrayList<>();
        for (int i = 0; i < selectNum; i++) {
            selectPaths.add(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + 0));
        }

        QueryExpression expression = QueryExpression.create();
        expression.setSelectSeries(selectPaths);
        if (useFilter) {
            QueryFilter queryFilter;
            if (align)
                queryFilter = new GlobalTimeFilter(TimeFilter.lt((long)(ptNum * selectRate)));
            else
                queryFilter = new GlobalTimeFilter(TimeFilter.lt((long) ((ptNum * selectRate+ 1) * sensorNum)));
            expression.setQueryFilter(queryFilter);
        }

        long startTime = System.currentTimeMillis();
        QueryDataSet dataSet = router.execute(expression);
        int cnt = 0;
        while (dataSet.hasNext()) {
//            System.out.println(dataSet.next().toString());
            dataSet.next();
            cnt++;
        }
        System.out.println(cnt);
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    private static void run() throws IOException {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            TsFileQuerier test = new TsFileQuerier();
            test.testQuery();
            totContumption += test.timeConsumption;
        }
        reportWriter.write(String.format("Query exp: \n"));
        reportWriter.write(String.format(filePath + "\t select rate: " +  selectRate + "\t select cols: " + selectNum + "\n"));
        reportWriter.write(String.format("Time consumption: %dms \n\n", totContumption / repetition));
//        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }

    // ts_lab2_x2_rate0.67.ts 0.5 5
    /**
     *
     * @param args filename, colNum, seleRate, ptNum
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
//        args = new String[]{};
        filePath = args[0];
        useFilter = true;
        selectNum = Integer.parseInt(args[1]);
        selectRate = Float.parseFloat(args[2]);
        ptNum = Long.parseLong(args[3]);
        repetition = 1;

        expReportFilePath = "tsfile_rpt";
        File f = new File(expReportFilePath);
        if(!f.exists()) f.createNewFile();
        reportWriter = new FileWriter(expReportFilePath, true);

        System.out.println(String.format("start : " + filePath + "\t select rate: " +  selectRate + "\t select cols: " + selectNum));
        run();
        System.out.println("end.......");
        reportWriter.close();
    }
}