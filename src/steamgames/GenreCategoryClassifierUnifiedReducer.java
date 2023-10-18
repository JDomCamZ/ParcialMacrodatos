/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class GenreCategoryClassifierUnifiedReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double positiveSum = 0.0;
        double negativeSum = 0.0;
        int count = 0;
        Map<String, Integer> subtitleLangCounts = new HashMap<>();
        Map<String, Integer> audioLangCounts = new HashMap<>();
        int windowsCount = 0;
        int macCount = 0;
        int linuxCount = 0;
        String mostPositivelyRatedGame = "";
        double maxPositiveRating = Double.MIN_VALUE;
        String mostNegativelyRatedGame = "";
        double maxNegativeRating = Double.MAX_VALUE;

        while (values.hasNext()) {
            Text value = values.next();
            String[] fields = customCSVSplit(value.toString());
            double positive = Double.parseDouble(fields[4]);
            double negative = Double.parseDouble(fields[5]);
            String subtitleLanguages = fields[1];
            String audioLanguages = fields[2];
            String availableOn = fields[3];

            positiveSum += positive;
            negativeSum += negative;
            count++;

            // Actualizar recuento de idiomas de subtítulos y audio
            updateLanguageCounts(subtitleLangCounts, subtitleLanguages);
            if (!audioLanguages.trim().equals("[]")) {
                updateLanguageCounts(audioLangCounts, audioLanguages);
            }

            // Calcular la disponibilidad
            if (availableOn.contains("Windows")) {
                windowsCount++;
            }
            if (availableOn.contains("Mac")) {
                macCount++;
            }
            if (availableOn.contains("Linux")) {
                linuxCount++;
            }

            double ratio = positive / negative;

            // Actualizar los juegos mejor y peor valorados
            if (ratio > maxPositiveRating) {
                maxPositiveRating = ratio;
                mostPositivelyRatedGame = fields[0];
            }
            if (ratio < maxNegativeRating) {
                maxNegativeRating = ratio;
                mostNegativelyRatedGame = fields[0];
            }
        }

        // Calcular promedios
        double positiveAvg = positiveSum / count;
        double negativeAvg = negativeSum / count;

        // Encontrar los idiomas más comunes
        List<String> mostCommonSubtitleLangs = findMostCommonLanguages(subtitleLangCounts, 3);
        List<String> mostCommonAudioLangs = findMostCommonLanguages(audioLangCounts, 2);

        // Calcular porcentaje de disponibilidad
        double windowsPercentage = (double) windowsCount / count * 100.0;
        double macPercentage = (double) macCount / count * 100.0;
        double linuxPercentage = (double) linuxCount / count * 100.0;

        // Generar la salida
        String outputValue = String.format("Positive Avg: %.2f Negative Avg: %.2f " +
            "Most Common Subtitle Lang: %s Most Common Audio Lang: %s " +
            "Windows: %.2f%% Mac: %.2f%% Linux: %.2f%% " +
            "Most Valorated Game: %s Most Hated Game: %s",
            positiveAvg, negativeAvg, mostCommonSubtitleLangs, mostCommonAudioLangs,
            windowsPercentage, macPercentage, linuxPercentage,
            mostPositivelyRatedGame, mostNegativelyRatedGame);

        output.collect(key, new Text(outputValue));
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
    
    // Función para actualizar el recuento de idiomas
    private void updateLanguageCounts(Map<String, Integer> langCounts, String languages) {
        String[] langArray = languages.split(",");
        for (String lang : langArray) {
            lang = lang.trim().replaceAll("[\\[\\]' ]", "");
            langCounts.put(lang, langCounts.getOrDefault(lang, 0) + 1);
        }
    }

    // Función para encontrar los idiomas más comunes
    private List<String> findMostCommonLanguages(Map<String, Integer> langCounts, int count) {
        return langCounts.entrySet().stream()
            .sorted((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()))
            .limit(count)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }
    
}
