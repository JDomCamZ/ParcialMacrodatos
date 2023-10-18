/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class TopGamesPerYearReducer  extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        double maxScore = 0;
        String bestGameInfo = "";
        
         try {
            while (values.hasNext()) {
             String[] parts = values.next().toString().split(",");
             if (parts.length >= 2) {
                double score = Double.parseDouble(parts[1]);
                if (score > maxScore) {
                    maxScore = score;
                    bestGameInfo = values.toString();
                }
            }
            
             
        }String[] partsInfo=bestGameInfo.split(",");
               output.collect(new Text(key), new Text(partsInfo[0]+" puntaje"+partsInfo[1]+" usuarios aprox"+partsInfo[2]+" precio"+partsInfo[3]+" géner(os)"+partsInfo[4]));
            
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}
