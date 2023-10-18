/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class GenreCategoryClassifierUnifiedMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    String fechainit, fechafin; 
    int ownerMin = 0;

    @Override
    public void configure(JobConf job) {
        // Recuperar los valores configurados en el método main
        fechainit = job.get("fechaInicio");
        fechafin = job.get("fechaFin");
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
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String[] fields = customCSVSplit(value.toString());
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
        if (fields.length > 20) {
            String priceString = fields[6].trim();
            if (!priceString.equals("Price")) {
                int sales = parseSales(fields[3].trim());
                if (sales < ownerMin) {
                    return;
                }    
                try {
                    String metacritic = fields[13].trim();
                    
                    // Parse Metacritic score and filter out games with a score of 0
                    double metacriticScore = Double.parseDouble(metacritic);
                    if (metacriticScore == 0) {
                        return; // Skip games with a Metacritic score of 0
                    }
                    
                    String date = fields[2];
                    
                    try {
                        
                        if (date != null && !date.isEmpty()) {
                            Date gameDate = dateFormat.parse(date);

                            if (gameDate != null) {
                                // Verifica si la fecha del juego está dentro del rango deseado
                                if (gameDate.after(fechaInicio) && gameDate.before(fechaFin)) {
                                    String name = "\"" + fields[1] + "\"";
                                    String subtitleLanguages = "\"" + fields[8] + "\"";
                                    String audioLanguages = "\"" + fields[9] + "\"";
                                    String availableOn = getAvailablePlatforms(fields[10], fields[11], fields[12]);
                                    String positive = fields[15];
                                    String negative = fields[16];
                                    String categories = fields[26];
                                    String genres = fields[27];
                                    String[] categoriesArray = categories.split(",");
                                    String[] genresArray = genres.split(",");
                                    for (String category : categoriesArray) {
                                        for (String genre : genresArray) {
                                            output.collect(new Text("[" + category.trim() + ", " + genre.trim() + "]"), new Text(String.join(",", name, subtitleLanguages, audioLanguages, availableOn, positive, negative)));
                                        }
                                    }
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
