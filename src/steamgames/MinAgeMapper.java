/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author User
 */
public class MinAgeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
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
         String[] fields = customCSVSplit(value.toString());
         if (fields.length > 20) {
            String priceString = fields[6].trim();
            if (!priceString.equals("Price")) {
                try {
                    String metacritic = fields[13].trim();
                    double metacriticScore = Double.parseDouble(metacritic);
                    if (metacriticScore == 0) {
                        return; // Skip games with a Metacritic score of 0
                    }
                    String age = fields[5].trim();
                    int minAge = Integer.parseInt(age);
                    if (minAge == 0) {
                        return;
                    }
                    int sales = parseSales(fields[3].trim());
                    String stats = metacritic + "," + sales + "," + fields[20];
                    output.collect(new Text(age), new Text(stats));
                }  catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }
        }
    }     
}
