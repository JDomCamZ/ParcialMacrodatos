/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class SalesGenreReducer  extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String mostSoldGame = "";
        long maxOwners = Long.MIN_VALUE;
        long totalOwners=Long.MIN_VALUE;
         try {
            while (values.hasNext()) {
             String[] parts = values.next().toString().split(",");
            String gameName = parts[0];
            long estimatedOwners = Long.parseLong(parts[1]);
            totalOwners+=estimatedOwners;
            // Check if this game has more estimated owners
            if (estimatedOwners > maxOwners) {
                maxOwners = estimatedOwners;
                mostSoldGame = gameName;
            }
        }
               output.collect(new Text(key), new Text("Total de ventas de este género"+totalOwners+"  juego más vendido"+mostSoldGame + "  total de ventas de este juego" + maxOwners));
            
        } catch (NumberFormatException e) {
            System.err.println("Error en la conversión de " + " " + " a double.");
        }
    }
}
