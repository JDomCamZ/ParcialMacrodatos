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
public class CategoryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    String category; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        category = job.get("category");
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
        if (parts.length >= 27) {  
                try {
                    int PlayForever=Integer.parseInt(parts[21]);
                    if(PlayForever<=0){//si no se juegoa
                        return;
                    }
                    
                    String[] allCategories=parts[27].split(",");
                    
                    for (String cat : allCategories) {
                       if(cat.equals(category))output.collect(new Text(cat), value);  
                    } 
                    
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
        }
    }
}
