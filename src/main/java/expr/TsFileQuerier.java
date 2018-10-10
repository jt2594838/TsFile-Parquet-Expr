package expr;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;
import hadoop.HDFSInputStream;
import static cons.Constants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        long startTime = System.currentTimeMillis();
        QueryDataSet dataSet = router.execute(expression);
        timeConsumption = System.currentTimeMillis() - startTime;
    }

    public static void main(String[] args) throws IOException {
        long totContumption = 0;
        for (int i = 0; i < repetition; i++) {
            TsFileQuerier test = new TsFileQuerier();
            test.testQuery();
            totContumption += test.timeConsumption;
        }
        System.out.println(String.format("Time consumption: %dms", totContumption / repetition));
    }
}