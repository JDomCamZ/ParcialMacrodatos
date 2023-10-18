/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author User
 */
public class SteamGames {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String consulta = args[0];
         switch (consulta) {
            case "price":
                priceanal(args[1], args[2]);
                break;
            case "dev":
                devanalysis(args[1], args[2]);
                break;
             case "dates":
                 datebigmap(args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
                 break;
             case "test":
                 test(args[1], args[2]);
                 break;
             case "genre":
                 // Lógica para la segunda consulta
                 SalesGenre(args[0], args[1]);
                 break;
             case "os":
                 Availability(args[0], args[1]);
                 break;
 /*           case "minmax":
                // Lógica para la segunda consulta
                MinMax(args[0], args[1]);
                break;
            case "dates":
                String fechaInit = args[3];
                String fechaFin = args[4];
                dates(args[0], args[1], fechaInit, fechaFin);
                break;
            case "geomean":
                geomean(args[0], args[1]);
                break;
            case "sub":
                String subcadena = args[3];
                SubString(args[0], args[1], subcadena);
                break;
            case "mode":
                Mode(args[0], args[1]);
                break;
            case "totproduct":
                String product = args[3];
                String payMeth = args[4];
                String rating = args[5];
                fechaInit = args[6];
                fechaFin = args[7];
                totproduct(args[0], args[1], product, payMeth, rating, fechaInit, fechaFin);
                break;
            case "cityspend":
                String ciudad = args[3];
                fechaInit = args[4];
                fechaFin = args[5];
                cityavgspend(args[0], args[1], ciudad, fechaInit, fechaFin);
                break;
            case "avgproduct":
                 Average(args[0], args[1]);
                 break;
            case "ID":
                String id = args[3];
                String cantidad = args[4];
                
                IdProducts(args[0],args[1],id,cantidad);
                break; */
            default:
                System.err.println("Consulta no válida: " + consulta);
                System.exit(1);
        }
    }
    
    static void priceanal(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("PriceAnalysis");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.PriceAnalysisGamesMapper.class);
        job_conf.setReducerClass(steamgames.PriceAnalysisGamesReducer.class);

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
    }

    static void devanalysis(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("DevAnalysis");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.DeveloperPublisherMapper.class);
        job_conf.setReducerClass(steamgames.DeveloperPublisherReducer.class);

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
    }


    static void datebigmap(String input, String output, String output2, String output3, String fechainit, String fechafin, String owners) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("DateBigMap");
        job_conf.set("fechaInicio", fechainit);
        job_conf.set("fechaFin", fechafin);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.DatesScoreMapper.class);
        job_conf.setReducerClass(steamgames.DatesScoreReducer.class);

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

        JobClient my_client2 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf2 = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf2.setJobName("OwnersChooseMap");
        job_conf2.set("ownerMin", owners);

        // Specify data type of output key and value
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf2.setMapperClass(steamgames.OwnerAmountMapper.class);
        job_conf2.setReducerClass(steamgames.OwnerAmountReducer.class);

        // Specify formats of the data type of Input and output
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf2, new Path(output));
        FileOutputFormat.setOutputPath(job_conf2, new Path(output2));

        my_client2.setConf(job_conf2);
        try {
            // Run the job
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        JobClient my_client3 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf3 = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf3.setJobName("GenreCategoryClassifier");

        // Specify data type of output key and value
        job_conf3.setOutputKeyClass(Text.class);
        job_conf3.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf3.setMapperClass(steamgames.GenreCategoryClassifierMapper.class);
        job_conf3.setReducerClass(steamgames.GenreCategoryClassifierReducer.class);

        // Specify formats of the data type of Input and output
        job_conf3.setInputFormat(TextInputFormat.class);
        job_conf3.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        //arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        FileInputFormat.setInputPaths(job_conf3, new Path(output2));
        FileOutputFormat.setOutputPath(job_conf3, new Path(output3));

        my_client3.setConf(job_conf3);
        try {
            // Run the job
            JobClient.runJob(job_conf3);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void test(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("Test");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.TestMapper.class);
        job_conf.setReducerClass(steamgames.TestReducer.class);

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
    }
    static void Availability(String input, String output) {
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
    }
    static void SalesGenre(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("SalesGenre");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.OwnersByGenreMapper.class);
        job_conf.setReducerClass(steamgames.OwnersByGenreReducer.class);

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
    }
    static void PublisherForDeveloperSales(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("SalesGenre");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.OwnersByGenreMapper.class);
        job_conf.setReducerClass(steamgames.OwnersByGenreReducer.class);

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
    }
}
