package expr;

import com.csvreader.CsvWriter;
import datagen.DataGenerator;
import datagen.GeneratorFactory;

import java.io.IOException;

public class DataGenPrepare {

    public static int append(int cur, int len, int max){
        int ans = (cur + len) % max;
        return ans == 0 ? max : ans;
    }
    /*input: ptNum, deviceNum, sensorNum, nullRate*/
    public static void main(String args[]) throws IOException {
        int lineNumber = Integer.parseInt(args[0]),
                deviceNumber = Integer.parseInt(args[1]),
                sensorNumber = Integer.parseInt(args[2]);
        float rate = Float.parseFloat(args[3]);
        String csvFilePath = "p" + lineNumber + "_d" + deviceNumber + "_s" + sensorNumber + "_r" + rate + ".csv";
        int cur_dev = 1;
        CsvWriter csvWriter = new CsvWriter(csvFilePath);
        DataGenerator dataGenerator = GeneratorFactory.INSTANCE.getGenerator();
        for(int i= 0; i < lineNumber; i++){
            String[] line = new String[deviceNumber*sensorNumber+1];
            line[0] = ""+(i+1);
            for(int j = 1; j <= deviceNumber*sensorNumber; j++){
                line[j] = ""+ (float)dataGenerator.next();
            }
            for(int j = 0; j < deviceNumber * rate * sensorNumber; j++){
                int k = (cur_dev - 1) * sensorNumber + 1 + j;
                k = k > (line.length - 1) ? k % (line.length - 1) : k;
                line[k] = "";
            }
            cur_dev = append(cur_dev, (int) (deviceNumber * rate), deviceNumber);
            csvWriter.writeRecord(line);
        }
        csvWriter.close();
    }
}
