/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DeveloperPublisherReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double metacriticSum = 0.0;
        double priceSum = 0.0;
        int totalSales = 0;
        double dlcSum = 0.0;
        int count = 0;

        while (values.hasNext()) {
            Text gameInfo = values.next();
            String[] parts = gameInfo.toString().split("\t");

            if (parts.length == 4) {
                double metacritic = Double.parseDouble(parts[0]);
                double price = Double.parseDouble(parts[1]);
                int sales = Integer.parseInt(parts[2]);
                double numDLCs = Double.parseDouble(parts[3]);

                metacriticSum += metacritic;
                priceSum += price;
                totalSales += sales;
                dlcSum += numDLCs;
                count++;
            }
        }

        if (count > 2) {
            double averageMetacritic = metacriticSum / count;
            double averagePrice = priceSum / count;

            if (averageMetacritic > 0) {
                String metacriticFormatted = String.format("%.1f", averageMetacritic);
                String priceFormatted = String.format("$%.2f", averagePrice);
                String outputValue = "Avg Metacritic: " + metacriticFormatted +
                        "\tAvg Price: " + priceFormatted +
                        "\tTotal Sales: " + totalSales +
                        "\tAvg DLCs: " + String.format("%.1f", dlcSum / count);

                output.collect(key, new Text(outputValue));
            }
        }
    }
}
