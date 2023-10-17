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
public class DeveloperPublisherMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final Text company = new Text();
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
    
    private int parseSales(String sales) {
        String[] parts = sales.split("-");
        if (parts.length == 2) {
            try {
                int lower = Integer.parseInt(parts[0].trim());
                int upper = Integer.parseInt(parts[1].trim());
                return (lower + upper) / 2;
            } catch (NumberFormatException e) {
                // Manejar errores de conversión si es necesario.
            }
        }
        // Valor predeterminado si no se puede analizar la entrada.
        return 0;
    }
    
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Parse the input record into genre and price (assuming a CSV format).
        String[] parts = customCSVSplit(value.toString());
        if (parts.length > 20) {
            String priceString = parts[6].trim();
            if (!priceString.equals("Price")) {
                try {
                    String metacritic = parts[13].trim();
                    
                    // Parse Metacritic score and filter out games with a score of 0
                    double metacriticScore = Double.parseDouble(metacritic);
                    if (metacriticScore == 0) {
                        return; // Skip games with a Metacritic score of 0
                    }
                    double priceValue = Double.parseDouble(priceString);
                    int sales = parseSales(parts[3].trim());
                    double numDLCs = Double.parseDouble(parts[7].trim());
                    String[] developers = parts[24].split(",");
                    for (String developer : developers) {
                        developer = developer.trim();
                        if (!developer.isEmpty()) {
                            String gameInfoValue = metacritic + "\t" + priceValue + "\t" + sales + "\t" + numDLCs;
                            gameInfo.set(gameInfoValue);

                            // Emit developer
                            output.collect(new Text("Developer: " + developer), gameInfo);
                        }
                    }
                    
                    // Emitir publisher
                    String[] publishers = parts[25].split(",");
                    for (String publisher : publishers) {
                        publisher = publisher.trim();
                        if (!publisher.isEmpty()) {
                            String gameInfoValue = metacritic + "\t" + priceValue + "\t" + sales + "\t" + numDLCs;
                            gameInfo.set(gameInfoValue);

                            // Emit publisher
                            output.collect(new Text("Publisher: " + publisher), gameInfo);
                        }
                    }
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }
        }
    }
}    
