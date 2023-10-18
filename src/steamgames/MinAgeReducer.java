/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
public class MinAgeReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double totalSales = 0;
        double totalPlayTime = 0;
        double totalMetacriticScore = 0;
        int count = 0;

        while (values.hasNext()) {
            String[] stats = values.next().toString().split(",");
            if (stats.length == 3) {
                totalSales += Double.parseDouble(stats[1]);
                totalPlayTime += Double.parseDouble(stats[2]);
                totalMetacriticScore += Double.parseDouble(stats[0]);
                count++;
            }
        }

        if (count > 0) {
            double averagePlayTime = totalPlayTime / count;
            double averageMetacriticScore = totalMetacriticScore / count;
            
            output.collect(key, new Text(String.format("Total de Ventas: %d  Tiempo Medio Jugado: %.2f Promedio de Puntuacion: %.2f", totalSales, averagePlayTime, averageMetacriticScore)));
        }
    }
}
