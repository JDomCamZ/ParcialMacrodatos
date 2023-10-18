/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package steamgames;

/**
 *
 * @author Miguel Huamani <miguel.huamani.r@uni.pe>
 */
public class Ayuda {
    /*
     case "genre":
                 OwnersGenre(args[1], args[2]);
                 break;
             case "os":
                 Availability(args[1], args[2]);
                 break;
             case "top":
                 Availability(args[1], args[2]);
                 break;
             case "gameinfo1":
                 FindGamesInfo1MR( args[1],args[2], args[3],args[4],args[5]);//args[2], args[3],args[4]=tag, categoría y precio mínimo
                 break;
             case "gameinfo3":
                 GameInfor3MR( args[1],args[2], args[3],args[4], args[5], args[6], args[7]);
                 break;
    
    
    
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
    */
}
