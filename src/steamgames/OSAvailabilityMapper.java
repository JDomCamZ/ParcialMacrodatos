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
            
            if(!fields[1].equals("Name"))output.collect(new Text("all"), new Text(Windows+","+Mac+","+Linuex));
               
            } catch (NumberFormatException e) {
                // Manejar el caso en el que no se pueda convertir a un número (puedes ignorarlo o registrar un error)
            }
        }
        
        
    }
    
}
/*static void Availability(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("Availability");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.OSAvailabilityMapper.class);
        job_conf.setReducerClass(steamgames.OSAvailabilityReducer.class);

        // Specify formats of the data type of Input and output
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf, new Path(input));
        FileOutputFormat.setOutputPath(job_conf, new Path(output));

        my_client.setConf(job_conf);
        try {
            // Run the job
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/