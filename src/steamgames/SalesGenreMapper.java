/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class SalesGenreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
    public String[] customCSVSplit(String input) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean insideQuotes = false;

        for (char c : input.toCharArray()) {
            if (c == '"') {
                insideQuotes = !insideQuotes;
            } else if (c == ',' && !insideQuotes) {
                // Si no estamos dentro de comillas, terminamos un campo.
                fields.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(c);
            }
        }

        // Asegúrate de agregar el último campo.
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }
    //de cada producto, conseguir las modas de la ciudad, el tipo de consumidor y el género, además del promedio de compras del producto
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        String[] fields =  customCSVSplit(value.toString());

        // Ensure the record is well-formed
        if (fields.length >= 28) {
            
            try {
                // Extract the game name, genres, and estimated owners
            String gameName = fields[1];
            String genres = fields[28];
            String estimatedOwners = fields[3];

            // Split genres into individual genres
            String[] genreList = genres.split(",");

            // Emit each genre along with the game name and estimated owners
            for (String genre : genreList) {
                output.collect(new Text(genre), new Text(gameName + "," + estimatedOwners));
            }
               
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
            }
        }
        
        
    }
}
