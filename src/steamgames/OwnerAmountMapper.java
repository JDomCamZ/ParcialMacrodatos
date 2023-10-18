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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author User
 */
public class OwnerAmountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    int ownerMin = 0;
    
    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        ownerMin = Integer.parseInt(job.get("ownerMin"));
    }
    
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
    
    private String getAvailablePlatforms(String windows, String mac, String linux) {
        // Crear una lista para almacenar las plataformas disponibles.
        List<String> platforms = new ArrayList<>();

        if (windows.equals("True")) {
            platforms.add("Windows");
        }
        if (mac.equals("True")) {
            platforms.add("Mac");
        }
        if (linux.equals("True")) {
            platforms.add("Linux");
        }

        return "\"[" + String.join(", ", platforms) + "]\"";
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
         String[] fields = customCSVSplit(value.toString());
         
         if (fields.length > 20) {
            String priceString = fields[6].trim();
            if (!priceString.equals("Price")) {
                try {
                    int sales = parseSales(fields[3].trim());
                    if (sales < ownerMin) {
                        return;
                    }    
                    String keyformiss = fields[0];
                    String name = "," + "\"" + fields[1] + "\"";
                    String subtitleLanguages = "\"" + fields[8] + "\"";
                    String audioLanguages = "\"" + fields[9] + "\"";
                    String availableOn = getAvailablePlatforms(fields[10], fields[11], fields[12]);
                    String positive = fields[15];
                    String negative = fields[16];
                    String categories = "\"" + fields[26] + "\"";
                    String genres = "\"" + fields[27] + "\"";
                    output.collect(new Text(keyformiss), new Text(String.join(",", name, subtitleLanguages, audioLanguages, availableOn, positive, negative, categories, genres)));
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }
        }
    }     
}
