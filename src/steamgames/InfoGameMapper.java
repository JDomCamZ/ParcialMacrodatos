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
/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class InfoGameMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    String price; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        price = job.get("price");
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
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
  
        String[] parts = customCSVSplit(value.toString());
        if (parts.length >= 6) {  
                try {
                    String game=parts[1];
                    double minPrice=Double.parseDouble(price);
                    double gamePrice=Double.parseDouble(parts[6].trim());
                    
                    if(gamePrice>=minPrice){//si no se juegoa
                        output.collect(new Text(game), value);
                    }
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
        }
    }
}
