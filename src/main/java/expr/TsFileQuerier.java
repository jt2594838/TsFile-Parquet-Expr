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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static cons.Constants.*;

public class TsFileQuerier {

    private QueryExecutorRouter router;
    private long timeConsumption;

    private void initEngine() throws IOException {
        ITsRandomAccessFileReader reader = new HDFSInputStream(filePath);
        router = new QueryExecutorRouter(new MetadataQuerierByFileImpl(reader),
                                         new SeriesChunkLoaderImpl(reader));
    }

    private void testQuery() throws IOException {
        initEngine();

        List<Path> selectPaths = new ArrayList<>();
        for (int i = 0; i < selectNum; i++) {
            selectPaths.add(new Path(DEVICE_PREFIX + i + SEPARATOR + SENSOR_PREFIX + i));
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
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }

    public static void main(String[] args) throws IOException {
        filePath = "expr2.ts";
        useFilter = false;
        ptNum = 10000;
        selectNum = 5;
        selectRate = 0.1;
        repetition = 1;
        run();
    }
}