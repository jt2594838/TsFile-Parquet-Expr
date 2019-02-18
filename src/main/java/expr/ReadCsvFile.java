package expr;


import com.csvreader.CsvReader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ReadCsvFile {

    public static void main(String args[]) throws IOException {
        String csvPath = args[0];
        String csvReadRpt = "csv_read_rpt";

        File f = new File(csvReadRpt);
        if(!f.exists()) f.createNewFile();
        FileWriter reportWriter = new FileWriter(f, true);

        CsvReader reader = new CsvReader(csvPath);

        long startTime = System.currentTimeMillis();
        while(reader.readRecord()){
            String[] line = reader.getValues();
            String s;
            for(int i = 0; i < line.length; i++)
                s = line[i];
        }
        reader.close();

        reportWriter.write(String.format("Reading FileName: " + csvPath + ", Time Consumption: "
                + (System.currentTimeMillis() - startTime)/1000 + "\n"));

        reportWriter.close();

    }

}
