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
public class TagCategoryMapper  extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    String tag; 
    String category;
    String price; 
    
    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        tag = job.get("tag");
        category = job.get("category");
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
        if (parts.length >= 29) {  
            String game=parts[1];
            if(!game.equals("Name")){
                try {
                    String metacritic = parts[13].trim();
                    double metacriticScore = Double.parseDouble(metacritic);
                    if (metacriticScore == 0) {
                        return; // Skip 
                    }
                    
                    int PlayForever=Integer.parseInt(parts[21]);
                    if(PlayForever<=0){//si no se juegoa
                        return;
                    }
                            // Emit publisher
                    String[] allTags=parts[29].split(",");
                    String[] allCategories=parts[27].split(",");
                    double minPrice=Double.parseDouble(price);
                    double gamePrice=Double.parseDouble(parts[6].trim());
                    
                    for (String oneTag : allTags) {
                       for(String cat :allCategories){
                           if(gamePrice>=minPrice && oneTag.equals(tag) &&cat.equals(category)){//si no se juegoa
                           output.collect(new Text(game), value);
                         }
                           
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
