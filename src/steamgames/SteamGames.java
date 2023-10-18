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
             case "litdates":
                 littledates(args[1], args[2], args[3], args[4], args[5]);
                 break;    
             case "test":
                 test(args[1], args[2]);
                 break;
             case "genre":
                 OwnersGenre(args[1], args[2]);
                 break;
             case "os":
                 Availability(args[1], args[2]);
                 break;
             case "top":
                 TopGame(args[1], args[2]);
                 break;
             case "gameinfo1":                                                            //ejem:     Wargame Single-player 20.00
                 FindGamesInfo1MR( args[1],args[2], args[3],args[4],args[5]);//args[3], args[4],args[5]=tag, categoría y precio mínimo
                 break;
             case "gameinfo3":
                 GameInfor3MR( args[1],args[2], args[3],args[4], args[5], args[6], args[7]);//args[5], args[6],args[7]=tag, categoría y precio mínimo
                 break;
             case "minage":
                 MinAge(args[1], args[2]);
                 break;
 
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

    static void littledates(String input, String output, String fechainit, String fechafin, String owners) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("DateBigMap");
        job_conf.set("fechaInicio", fechainit);
        job_conf.set("fechaFin", fechafin);
        job_conf.set("ownerMin", owners);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.GenreCategoryClassifierUnifiedMapper.class);
        job_conf.setReducerClass(steamgames.GenreCategoryClassifierUnifiedReducer.class);

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
    static void OwnersGenre(String input, String output) {
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
    
    static void MinAge(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("MinAge");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.MinAgeMapper.class);
        job_conf.setReducerClass(steamgames.MinAgeReducer.class);

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
    static void TopGame(String input, String output) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("Availability");

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.TopGamesPerYearMapper.class);
        job_conf.setReducerClass(steamgames.TopGamesPerYearReducer.class);

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
    
    static void FindGamesInfo1MR(String input, String output,String tag, String category, String price) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("Availability");
        job_conf.set("tag", tag);
        job_conf.set("category", category);
        job_conf.set("price",price);
        
        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.TagCategoryMapper.class);
        job_conf.setReducerClass(steamgames.TagCategoryReducer.class);

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
    static void GameInfor3MR(String input, String output, String output2, String output3, String tag, String category, String price) {
        JobClient my_client = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf.setJobName("GameTag");
        job_conf.set("tag", tag);

        // Specify data type of output key and value
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf.setMapperClass(steamgames.TagMapper.class);
        job_conf.setReducerClass(steamgames.TagReducer.class);

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
        ///2DO MAP REDUCE ----------------------------------------------------------------------------------------------------------------
        JobClient my_client2 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf2 = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf2.setJobName("GameCategory");
        job_conf2.set("category", category);

        // Specify data type of output key and value
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf2.setMapperClass(steamgames.CategoryMapper.class);
        job_conf2.setReducerClass(steamgames.CategoryReducer.class);

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
         
         ///3ER MAP REDUCE ----------------------------------------------------------------------------------------------------------------
        JobClient my_client3 = new JobClient();
        // Create a configuration object for the job
        JobConf job_conf3 = new JobConf(SteamGames.class);

        // Set a name of the Job
        job_conf3.setJobName("Info");
        job_conf3.set("price", price);

        // Specify data type of output key and value
        job_conf3.setOutputKeyClass(Text.class);
        job_conf3.setOutputValueClass(Text.class);

        // Specify names of Mapper and Reducer Class
        job_conf3.setMapperClass(steamgames.InfoGameMapper.class);
        job_conf3.setReducerClass(steamgames.InfoGameReducer.class);

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

}
