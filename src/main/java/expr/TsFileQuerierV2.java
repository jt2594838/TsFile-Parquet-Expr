package expr;

import static cons.Constants.selectNum;
import static cons.Constants.selectRate;

public class TsFileQuerierV2 {
    // parquet_lab1_x2_rate0.0.parquet 0.5
    public static void main(String args[]){
        String filePath = args[0];
        selectRate = Float.parseFloat(args[1]);
        selectNum = 5;
    }
}
