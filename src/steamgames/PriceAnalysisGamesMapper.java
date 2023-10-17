/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class PriceAnalysisGamesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    /*private final Text genre = new Text();
    private final DoubleWritable price = new DoubleWritable(0);*/
    
    
    private final Text genre = new Text();
    private final Text gameInfo = new Text();
    
    
    
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
    /*
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Parse the input record into genre and price (assuming a CSV format).
        String[] parts = customCSVSplit(value.toString());
        if (parts.length > 33) {
            String priceString = parts[6];
            if (!priceString.equals("Price")) {
                try {
                    double priceValue = Double.parseDouble(priceString);
                    String[] genres = parts[35].split(",");
                    String gameName = parts[1]; // Assuming the game name is in the 2nd column (parts[1]).
                    for (String genreStr : genres) {
                        genre.set(genreStr);
                        gameInfo.set(gameName + "\t" + priceValue);
                        output.collect(genre, gameInfo);
                    }
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }        
        }
    }*/
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Parse the input record into genre and price (assuming a CSV format).
        String[] parts = customCSVSplit(value.toString());
        if (parts.length > 20) {
            String priceString = parts[6];
            if (!priceString.equals("Price")) {
                try {
                    double priceValue = Double.parseDouble(priceString);
                    String[] genres = parts[27].split(",");
                    String gameName = parts[1];
                    for (String genr : genres) {
                        genre.set(genr);
                        gameInfo.set(gameName + "\t" + priceValue);
                        output.collect(genre, gameInfo);
                    }
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }        
        }
    }
}
