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
public class TopGamesPerYearMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
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
     private int getYearFromDate(String date) {
        String[] parts = date.split(" ");
        if (parts.length >= 3) {
            String yearPart = parts[2];
            return Integer.parseInt(yearPart);
        }
        return -1;
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        String[] fields = value.toString().split(",");

        // Ensure the record is well-formed
        if (fields.length >= 28) {
            
            try {
            if (fields.length >= 24) {
            int year = getYearFromDate(fields[2]); // Obtén el año
            String name=fields[1];
            String Score=fields[13];
            String owner=fields[3];
            String price=fields[6];
            String gener=fields[28];
            String gameInfo = name + "," + Score + "," + owner + "," + price + "," + gener;
            // Emite (año, información del juego)
            if(year>0 && !name.equals("Name"))output.collect(new Text(""+year), new Text(gameInfo));
           }
               
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
            }
        }
        
        
    }
    
}
