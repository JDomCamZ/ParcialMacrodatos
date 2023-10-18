/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class DatesScoreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    String fechainit, fechafin; 

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        fechainit = job.get("fechaInicio");
        fechafin = job.get("fechaFin");
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
    
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        // Parse the input record into genre and price (assuming a CSV format).
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String[] parts = customCSVSplit(value.toString());
        Date fechaInicio = null;
        Date fechaFin = null;

        try {
            fechaInicio = dateFormat.parse(fechainit); // Fecha de inicio
            fechaFin = dateFormat.parse(fechafin);    // Fecha de fin
        } catch (ParseException e) {
            // Manejo de errores al analizar las fechas
            e.printStackTrace();
            return;
        }
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
                    
                    String date = parts[2];
                    
                    try {
                        
                        if (date != null && !date.isEmpty()) {
                            Date gameDate = dateFormat.parse(date);

                            if (gameDate != null) {
                                // Verifica si la fecha del juego está dentro del rango deseado
                                if (gameDate.after(fechaInicio) && gameDate.before(fechaFin)) {
                                    output.collect(new Text(date), value);
                                }
                            }
                        }
                    } catch (ParseException e) {
                                // Manejo de errores al analizar la fecha del juego
                                e.printStackTrace();
                    }
                } catch (NumberFormatException e) {
                    // Handle any parsing errors if the "price" column doesn't contain a valid double.
                    // You can log an error or take other appropriate actions.
                }
            }
        }
    }
}
