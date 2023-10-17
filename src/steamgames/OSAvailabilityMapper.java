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
public class OSAvailabilityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
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
        
        String[] fields = value.toString().split(",");

        // Ensure the record is well-formed
        if (fields.length >= 28) {
            
            try {
                // Extract the game name, genres, and estimated owners
            String Windows = fields[11];
            String Mac = fields[12];
            String Linuex = fields[13];

            // Emit each genre along with the game name and estimated owners
            
            output.collect(new Text("all"), new Text(Windows+","+Mac+","+Linuex));
               
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
            }
        }
        
        
    }
    
}
