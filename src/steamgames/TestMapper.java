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
public class TestMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
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
    
     @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] fields = customCSVSplit(value.toString());
        if (fields.length >= 7) {    
        
            String subLanguage = fields[0];
            String audioLanguage = fields[1].trim();
            String availableOn = fields[2].trim();
            String positive = fields[3].trim();
            String negative = fields[4].trim();
            String categories = fields[fields.length - 1];
            String[] categoriesArray = categories.split(","); // Separa las categorías
            String genres = fields[fields.length - 2];
            String[] genresArray = genres.split(","); // Separa los géneros

            output.collect(new Text(subLanguage + "\t" + audioLanguage + "\t" + availableOn + "\t" + positive + "\t" + negative), new Text(subLanguage + "\t" + audioLanguage + "\t" + availableOn + "\t" + positive + "\t" + negative));
        }
    }
}
