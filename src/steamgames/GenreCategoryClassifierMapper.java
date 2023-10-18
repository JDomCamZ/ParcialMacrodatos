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
public class GenreCategoryClassifierMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    
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
            // subLanguage contiene tanto el nombre del juego como los idiomas en subtitulos
            String gameName = fields[1].trim();
            String subLanguage = fields[2].trim();
            String audioLanguage = fields[3].trim();
            String availableOn = fields[4].trim();
            String positive = fields[5].trim();
            String negative = fields[6].trim();
            String categories = fields[fields.length - 1];
            String[] categoriesArray = categories.split(","); // Separa las categorías
            String genres = fields[fields.length - 2];
            String[] genresArray = genres.split(","); // Separa los géneros

            // Loop a través de categorías y géneros para emitir pares clave-valor
            for (String category : categoriesArray) {
                for (String genre : genresArray) {
                    output.collect(new Text("[" + category.trim() + ", " + genre.trim() + "]"), new Text(gameName + "\t" + subLanguage + "\t" + audioLanguage + "\t" + availableOn + "\t" + positive + "\t" + negative));
                }
            }
        }    
    }
    
}
