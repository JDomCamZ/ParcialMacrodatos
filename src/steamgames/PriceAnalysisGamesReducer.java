/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.Iterator;

public class PriceAnalysisGamesReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    /*@Override
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sum = 0;
        int count = 0;
        
        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }

        double average = sum / count;
        output.collect(key, new DoubleWritable(average));
    }*/

    
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double sum = 0;
        int count = 0;
        double maxPrice = Double.MIN_VALUE;
        double minPrice = Double.MAX_VALUE;
        String maxGame = "";
        String minGame = "";

        while (values.hasNext()) {
            String[] parts = values.next().toString().split("\t");
            double price = Double.parseDouble(parts[1]);
            sum += price;
            count++;
            
            if (price > maxPrice) {
                maxPrice = price;
                maxGame = parts[0];
            }
            
            if (price < minPrice) {
                minPrice = price;
                minGame = parts[0];
            }
        }

        double average = sum / count;
        String result = "Precio Promedio: $" + average + ", Juego Más Caro: " + maxGame + " ($" + maxPrice + "), Juego Más Barato: " + minGame + " ($" + minPrice + ")";
        output.collect(key, new Text(result));
        }
}
